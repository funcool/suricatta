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
           org.jooq.Configuration
           clojure.lang.PersistentVector
           clojure.lang.APersistentMap
           suricatta.types.Context))

(defn translate-dialect
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
    (let [datasource (:datasource dbspec)
          connection (if datasource
                       (.getConnection datasource)
                       (-> (jdbc/make-connection dbspec)
                           (:connection)))
          dialect (if (:dialect dbspec)
                    (translate-dialect (:dialect dbspec))
                    (JDBCUtils/dialect connection))]
      (->> (doto (DefaultConfiguration.)
             (.set dialect)
             (.set connection))
           (types/->context connection))))

  java.sql.Connection
  (make-context [^java.sql.Connection connection]
    (let [^SQLDialect dialect (JDBCUtils/dialect connection)]
      (->> (doto (DefaultConfiguration.)
             (.set dialect)
             (.set connection))
           (types/->context connection))))

  jdbc.types.Connection
  (make-context [^jdbc.types.Connection connection]
    (proto/make-context (:connection connection))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IExecute implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IExecute
  String
  (execute [^String sql ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)]
      (.execute context sql)))

  org.jooq.Query
  (execute [^org.jooq.Query query ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)]
      (.execute context query)))

  PersistentVector
  (execute [^PersistentVector sqlvec ^Context ctx]
    (let [^DSLContext context   (proto/get-context ctx)
          ^org.jooq.Query query (->> (into-array Object (rest sqlvec))
                                     (.query context (first sqlvec)))]
      (.execute context query)))

  suricatta.types.Deferred
  (execute [deferred ctx]
    (proto/execute @deferred ctx)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetch Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- keywordize-keys
  "Recursively transforms all map keys from strings to keywords."
  [m]
  (into {} (map (fn [[k v]] [(keyword (.toLowerCase k)) v]) m)))

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

  org.jooq.ResultQuery
  (fetch [^org.jooq.ResultQuery query ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)]
      (-> (.fetch context query)
          (result->vector opts))))

  PersistentVector
  (fetch [^PersistentVector sqlvec ^Context ctx opts]
    (let [^DSLContext context (proto/get-context ctx)
          ^org.jooq.ResultQuery query (->> (into-array Object (rest sqlvec))
                                           (.resultQuery context (first sqlvec)))]
      (-> (.fetch context query)
          (result->vector opts))))

  suricatta.types.Deferred
  (fetch [deferred ctx opts]
    (proto/fetch @deferred ctx opts)))
