(ns suricatta.impl
  (:require [jdbc.core :as jdbc]
            [jdbc.types :as jdbctypes]
            [suricatta.types :as types]
            [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration
           org.jooq.tools.jdbc.JDBCUtils
           org.jooq.SQLDialect
           org.jooq.DSLContext
           org.jooq.Configuration
           clojure.lang.PersistentVector
           clojure.lang.APersistentMap
           suricatta.types.Context
           suricatta.types.Query
           suricatta.types.ResultQuery))

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
      :mysql      SQLDialect/MYSQL
      :h2         SQLDialect/H2)))

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
;; Query Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IQuery
  String
  (query [^String sql ^Context ctx]
    (let [^DSLContext    context (proto/get-context ctx)
          ^Configuration conf    (.-conf ctx)]
      (-> (.query context sql)
          (types/->query conf))))

  PersistentVector
  (query [^PersistentVector sqlvec ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (.-conf ctx)]
      (-> (->> (rest sqlvec)
               (into-array Object)
               (.query context (first sqlvec)))
          (types/->query conf))))

  org.jooq.impl.AbstractQueryPart
  (query [^org.jooq.impl.AbstractQueryPart q ^Context ctx]
    (let [^Configuration conf (.-conf ctx)]
      (types/->query q conf))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Result Query Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IResultQuery
  String
  (result-query [^String sql ^Context ctx _]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (.-conf ctx)]
      (-> (.resultQuery context sql)
          (types/->result-query conf))))

  PersistentVector
  (result-query [^PersistentVector sqlvec ^Context ctx _]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (.-conf ctx)]
      (-> (->> (rest sqlvec)
               (into-array Object)
               (.resultQuery context (first sqlvec)))
          (types/->result-query conf)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IExecute implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IExecute
  String
  (execute [^String sql ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)]
      (.execute context sql)))

  Query
  (execute [^Query q ^Context ctx]
    (let [^DSLContext context (proto/get-context q)]
      (.execute context (:q q))))

  PersistentVector
  (execute [^PersistentVector sqlvec ^Context ctx]
    (let [query (proto/query sqlvec ctx)]
      (proto/execute query ctx))))


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

  ResultQuery
  (fetch [^ResultQuery q ^Context ctx opts]
    (let [^DSLContext context (proto/get-context q)]
      (-> (.fetch context (:q q))
          (result->vector opts))))

  PersistentVector
  (fetch [^PersistentVector sqlvec ^Context ctx opts]
    (let [q (proto/result-query sqlvec ctx opts)]
      (proto/fetch q ctx opts))))
