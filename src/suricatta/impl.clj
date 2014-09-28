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
  (case dialect
    :postgresql SQLDialect/POSTGRES
    :postgres   SQLDialect/POSTGRES
    :pgsql      SQLDialect/POSTGRES))

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
           (Context. connection))))

  java.sql.Connection
  (make-context [^java.sql.Connection connection]
    (let [^SQLDialect dialect (JDBCUtils/dialect connection)]
      (->> (doto (DefaultConfiguration.)
             (.set dialect)
             (.set connection))
           (Context. connection))))

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
          (Query. conf))))

  PersistentVector
  (query [^PersistentVector sqlvec ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (.-conf ctx)]
      (-> (->> (rest sqlvec)
               (into-array Object)
               (.query context (first sqlvec)))
          (Query. conf))))

  org.jooq.impl.AbstractQueryPart
  (query [^org.jooq.impl.AbstractQueryPart q ^Context ctx]
    (let [^Configuration conf (.-conf ctx)]
      (Query. q conf))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Result Query Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IResultQuery
  String
  (result-query [^String sql ^Context ctx _]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (.-conf ctx)]
      (-> (.resultQuery context sql)
          (ResultQuery. conf))))

  PersistentVector
  (result-query [^PersistentVector sqlvec ^Context ctx _]
    (let [^DSLContext context (proto/get-context ctx)
          ^Configuration conf (.-conf ctx)]
      (-> (->> (rest sqlvec)
               (into-array Object)
               (.resultQuery context (first sqlvec)))
          (ResultQuery. conf)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Convenience implementation for IExecute
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Convenience implementations for make easy executing
;; simple queries directly without explictly creating
;; a Query instance.

(extend-protocol proto/IExecute
  String
  (execute [^String sql ^Context ctx]
    (let [^DSLContext context (proto/get-context ctx)]
      (.execute context sql)))

  PersistentVector
  (execute [^PersistentVector sqlvec ^Context ctx]
    (let [query (proto/query sqlvec ctx)]
      (proto/execute query ctx))))
