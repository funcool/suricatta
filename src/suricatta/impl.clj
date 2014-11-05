;; Copyright (c) 2014, Andrey Antukh
;; All rights reserved.
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions are met:
;;
;; * Redistributions of source code must retain the above copyright notice, this
;;   list of conditions and the following disclaimer.
;;
;; * Redistributions in binary form must reproduce the above copyright notice,
;;   this list of conditions and the following disclaimer in the documentation
;;   and/or other materials provided with the distribution.
;;
;; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;; AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;; IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
;; DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
;; FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
;; DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
;; SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
;; CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
;; OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
;; OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

(ns suricatta.impl
  (:require [jdbc.core :as jdbc]
            [jdbc.types :as jdbctypes]
            [jdbc.proto :as jdbcproto]
            [suricatta.types :as types :refer [defer]]
            [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration
           org.jooq.tools.jdbc.JDBCUtils
           org.jooq.SQLDialect
           org.jooq.DSLContext
           org.jooq.ResultQuery
           org.jooq.Query
           org.jooq.Configuration
           clojure.lang.PersistentVector
           clojure.lang.APersistentMap
           java.sql.Connection
           javax.sql.DataSource
           suricatta.types.Context))

(defn ^SQLDialect translate-dialect
  "Translate keyword dialect name to proper
  jooq SQLDialect enum value."
  [dialect]
  (if (instance? SQLDialect dialect)
    dialect
    (case dialect
      :postgresql SQLDialect/POSTGRES
      :postgres   SQLDialect/POSTGRES
      :pgsql      SQLDialect/POSTGRES
      :mariadb    SQLDialect/MARIADB
      :firebird   SQLDialect/FIREBIRD
      :mysql      SQLDialect/MYSQL
      :h2         SQLDialect/H2
      :sqlite     SQLDialect/SQLITE
      SQLDialect/SQL99)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Context Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IContextBuilder
  APersistentMap
  (make-context [^clojure.lang.APersistentMap dbspec]
    (let [^DataSource datasource (:datasource dbspec)
          ^Connection connection (if datasource
                                   (.getConnection datasource)
                                   (-> (jdbc/make-connection dbspec)
                                       (:connection)))
          ^SQLDialect dialect (if (:dialect dbspec)
                                (translate-dialect (:dialect dbspec))
                                (JDBCUtils/dialect connection))
          ^Configuration conf (doto (DefaultConfiguration.)
                                (.set dialect)
                                (.set connection))]
      (types/->context conf)))

  java.sql.Connection
  (make-context [^Connection connection]
    (let [^SQLDialect dialect (JDBCUtils/dialect connection)
          ^Configuration conf (doto (DefaultConfiguration.)
                                (.set dialect)
                                (.set connection))]
      (types/->context conf)))

  jdbc.types.Connection
  (make-context [^jdbc.types.Connection connection]
    (proto/make-context (:connection connection))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IExecute implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IExecute
  java.lang.String
  (execute [^String sql ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)]
      (.execute context sql)))

  org.jooq.Query
  (execute [^Query query ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)]
      (.execute context query)))

  clojure.lang.PersistentVector
  (execute [^PersistentVector sqlvec ^Context ctx]
    (let [^DSLContext context   (proto/get-context ctx)
          ^Query query (.query context
                               (first sqlvec)
                               (into-array Object (rest sqlvec)))]
      (.execute context query)))

  suricatta.types.Deferred
  (execute [deferred ctx]
    (proto/execute @deferred ctx))

  suricatta.types.Query
  (execute [query ctx]
    (let [^DSLContext context (if (nil? ctx)
                                (proto/get-context query)
                                (proto/get-context ctx))
          ^ResultQuery query  (.-query query)]
      (.execute context query))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetch Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- keywordize-keys
  "Recursively transforms all map keys from strings to keywords."
  [m]
  (into {} (map (fn [[k v]] [(keyword (.toLowerCase ^String k)) v]) m)))

(defn- result-record->record
  [^org.jooq.Record record]
  (keywordize-keys (.intoMap record)))

(defn- result-record->row
  [^org.jooq.Record record]
  (into [] (.intoArray record)))

(defn- result->vector
  [^org.jooq.Result result {:keys [rows mapfn] :or {rows false}}]
  (cond
   mapfn (mapv mapfn result)
   rows  (mapv result-record->row result)
   :else (mapv result-record->record result)))

(extend-protocol proto/IFetch
  String
  (fetch [^String sql ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)]
      (-> (.fetch context sql)
          (result->vector opts))))

  PersistentVector
  (fetch [^PersistentVector sqlvec ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)
          ^ResultQuery query (->> (into-array Object (rest sqlvec))
                                  (.resultQuery context (first sqlvec)))]
      (-> (.fetch context query)
          (result->vector opts))))

  org.jooq.ResultQuery
  (fetch [^ResultQuery query ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)]
      (-> (.fetch context query)
          (result->vector opts))))

  suricatta.types.Deferred
  (fetch [deferred ctx opts]
    (proto/fetch @deferred ctx opts))

  suricatta.types.Query
  (fetch [query ctx opts]
    (let [^DSLContext context (if (nil? ctx)
                                (proto/get-context query)
                                (proto/get-context ctx))
          ^ResultQuery query  (.-query query)]
      (-> (.fetch context query)
          (result->vector opts)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetchLazy Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IFetchLazy
  java.lang.String
  (fetch-lazy [^String query ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)
          ^ResultQuery query  (.resultQuery context query)]
      (.fetchSize query (get opts :fetch-size 60))
      (.fetchLazy context query)))

  clojure.lang.PersistentVector
  (fetch-lazy [^PersistentVector sqlvec ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)
          ^ResultQuery query (->> (into-array Object (rest sqlvec))
                                  (.resultQuery context (first sqlvec)))]
      (.fetchSize query (get opts :fetch-size 60))
      (.fetchLazy context query)))

  org.jooq.ResultQuery
  (fetch-lazy [^ResultQuery query ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)]
      (.fetchSize query (get opts :fetch-size 60))
      (.fetchLazy context query)))

  suricatta.types.Deferred
  (fetch-lazy [deferred ctx opts]
    (proto/fetch-lazy @deferred ctx opts)))

(defn cursor->lazyseq
  [cursor {:keys [rows mapfn] :or {rows false}}]
  (let [lseq (fn thisfn []
               (when (.hasNext cursor)
                 (let [record (.fetchOne cursor)
                       record (cond
                               mapfn (mapfn record)
                               rows  (result-record->row record)
                               :else (result-record->record record))]
                   (cons record (lazy-seq (thisfn))))))]
    (lseq)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IQuery Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IQuery
  java.lang.String
  (query [sql ctx]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (proto/get-configuration ctx)
          ^ResultQuery query  (-> (.resultQuery context sql)
                                  (.keepStatement true))]
      (types/->query query conf)))

  PersistentVector
  (query [sqlvec ctx]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (proto/get-configuration ctx)
          ^ResultQuery query  (->> (into-array Object (rest sqlvec))
                                   (.resultQuery context (first sqlvec)))]
      (-> (doto query
            (.keepStatement true))
          (types/->query conf)))))
