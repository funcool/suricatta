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
           org.jooq.Field
           org.jooq.Result
           org.jooq.VisitContext
           org.jooq.RenderContext
           org.jooq.BindContext
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

(defn make-param-impl
  "Wraps a value that implements IParamType
  protocol in valid jOOQ Param implementation."
  [value]
  (->
   (reify suricatta.impl.IParam
     (render [_ ^RenderContext ctx]
       (let [sql (proto/render value)]
         (.sql ctx sql)))
     (bind [_ ^BindContext ctx]
       (let [stmt  (.statement ctx)
             index (.nextIndex ctx)]
         (proto/bind value stmt index))))
   (suricatta.impl.ParamWrapper.)))

(defn wrap-if-need
  [obj]
  (if (satisfies? proto/IParamType obj)
    (make-param-impl obj)
    obj))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Context Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IContextBuilder
  APersistentMap
  (make-context [^clojure.lang.APersistentMap dbspec]
    (let [^Connection connection (:connection (jdbc/make-connection dbspec))
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
    (let [^DSLContext context (proto/get-context ctx)
          ^Query query        (->> (into-array Object (map wrap-if-need (rest sqlvec)))
                                   (.query context (first sqlvec)))]
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

(defn- result-record->record
  [^org.jooq.Record record]
  (into {} (for [^int i (range (.size record))]
             (let [^Field field (.field record i)
                   value (.getValue record i)]
               [(keyword (.toLowerCase (.getName field)))
                (if (satisfies? proto/ISQLType value)
                  (proto/convert value)
                  value)]))))

(defn- result-record->row
  [^org.jooq.Record record]
  (into [] (for [^int i (range (.size record))]
             (let [value (.getValue record i)]
               (if (satisfies? proto/ISQLType value)
                  (proto/convert value)
                  value)))))

(defn- result->vector
  [^org.jooq.Result result {:keys [mapfn into format]
                            :or {rows false format :record}}]
  (if mapfn
    (mapv mapfn result)
    (condp = format
      :record (mapv result-record->record result)
      :row    (mapv result-record->row result)
      :json   (if into
                (.formatJSON result into)
                (.formatJSON result))
      :csv    (if into
                (.formatCSV result into)
                (.formatCSV result)))))

(extend-protocol proto/IFetch
  String
  (fetch [^String sql ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)
          ^Result result (.fetch context sql)]
      (result->vector result opts)))

  PersistentVector
  (fetch [^PersistentVector sqlvec ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)
          ^ResultQuery query (->> (into-array Object (map wrap-if-need (rest sqlvec)))
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
      (.fetchSize query (get opts :fetch-size 100))
      (.fetchLazy context query)))

  org.jooq.ResultQuery
  (fetch-lazy [^ResultQuery query ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)]
      (.fetchSize query (get opts :fetch-size 100))
      (.fetchLazy context query)))

  suricatta.types.Deferred
  (fetch-lazy [deferred ctx opts]
    (proto/fetch-lazy @deferred ctx opts)))

(defn cursor->lazyseq
  [cursor {:keys [format mapfn] :or {format :record}}]
  (let [lseq (fn thisfn []
               (when (.hasNext cursor)
                 (let [item (.fetchOne cursor)
                       record (condp = format
                                :record (result-record->record item)
                                :row (result-record->row item))]
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load into implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-into
  [ctx tablename data {:keys [format commit fields ignore-rows
                              nullstring quotechar separator]
                       :or {format :csv commit :none ignore-rows 0
                            nullstring "" quotechar \" separator \,}}]
  (let [^DSLContext context (proto/get-context ctx)
        step (.loadInto context (DSL/table (name tablename)))
        step (condp = commit
               :none (.commitNone step)
               :each (.commitEach step)
               :all  (.commitAll step)
               (.commitAfter step commit))
        step (condp = format
               :csv  (.loadCSV step data)
               :json (.loadJSON step data))
        ;; fields (->> (map #(DSL/field (name %)) fields)
        ;;             (into-array org.jooq.Field))
        fields (into-array org.jooq.Field fields)]
    (doto step
      (.fields fields)
      (.ignoreRows ignore-rows))
    (when (= format :csv)
      (doto step
        (.quote quotechar)
        (.nullString nullstring)
        (.separator separator)))
    (.execute step)))
