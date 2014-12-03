(ns suricatta.extend-test
  (:require [clojure.test :refer :all]
            [suricatta.core :as sc]
            [suricatta.dsl :as dsl]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [cheshire.core :refer :all])
  (:import org.postgresql.util.PGobject
           org.jooq.impl.DSL))

(def dbspec {:subprotocol "postgresql"
             :subname "//127.0.0.1/test"})

(def ^:dynamic *ctx*)

(defn my-fixture
  [end]
  (with-open [ctx (sc/context dbspec)]
    (sc/atomic ctx
      (binding [*ctx* ctx]
        (end)
        (sc/set-rollback! ctx)))))

(use-fixtures :each my-fixture)

;; (defn json
;;   [data]
;;   (proxy [org.jooq.impl.CustomField] [nil, org.jooq.util.postgres.PostgresDataType/JSON]
;;     (accept [ctx]
;;       (cond
;;        (instance? org.jooq.RenderContext ctx)
;;        (doto ctx
;;          (.sql "'")
;;          (.sql (generate-string data))
;;          (.sql "'::json"))

;;        (instance? org.jooq.BindContext ctx)
;;        (let [obj        (doto (PGobject.)
;;                           (.setType "json")
;;                           (.setValue (generate-string data)))
;;              statement  (.statement ctx)
;;              _          (println 454545 (class (.getDelegate statement)))
;;              paramcount (.getParameterCount (.getParameterMetaData statement))]
;;         (when (<= (.peekIndex ctx) paramcount)
;;           (.setObject statement
;;                       (.nextIndex ctx)
;;                       obj)))))))


;; (defn json
;;   [data]
;;   (-> (str "'" (generate-string data) "'::json")
;;       (DSL/queryPart)))

;; (defn json
;;   [data]
;;   (proxy [org.jooq.impl.CustomField] [nil org.jooq.util.postgres.PostgresDataType/JSON]
;;     (toSQL [context]
;;       (if (.inline context)
;;         (doto context
;;           (.sql "'")
;;           (.sql (generate-string data))
;;           (.sql "'::json"))
;;         (.sql context "?")))
;;     (bind [context]
;;       (let [statement (.statement context)
;;             next-index (.nextIndex context)]
;;         (.setObject statement next-index (doto (PGobject.)
;;                                            (.setType "json")
;;                                            (.setValue (generate-string data))))))))


;; (deftest inserting-json
;;   (sc/execute *ctx* "create table t1 (data json, k int)")
;;   (sc/execute *ctx* ["insert into t1 (data, k) values (?, ?)" (json {:foo "bar"}) 2])
;;   (-> (sc/fetch *ctx* "select * from t1")
;;       (println)))

;; (deftest inserting-array
;;   (sc/execute *ctx* "create table t1 (data json, k int)")
;;   (sc/execute *ctx* ["insert into t1 (data, k) values ({0}, {1})" (json {:foo "bar"}) 2])
;;   (-> (sc/fetch *ctx* "select * from t1")
;;       (println)))


;; (deftest inserting-json-dsl
;;   (sc/execute *ctx* "create table t1 (data json)")
;;   (let [q (-> (dsl/insert-into :t1)
;;               (dsl/insert-values {:data (json {:foo 1})})
;;               (dsl/insert-values {:data (json {:bar 2})}))]
;;     (sc/execute *ctx* q))
;;   (-> (sc/fetch *ctx* "select * from t1")
;;       (println)))

(def datatype (org.jooq.impl.DefaultDataType/getDefaultDataType "__other"))

;; (deftest inserting-json
;;   (sc/execute *ctx* "create table t1 (data json, k int)")
;;   (sc/execute *ctx* ["insert into t1 (data, k) values (?, ?)" (DSL/val #{:foo "bar"} datatype) 3])
;;   (-> (sc/fetch *ctx* ["select * from t1"])
;;       (println)))

(deftest inserting-json
  (sc/execute *ctx* "create table t1 (k int)")
  (sc/execute *ctx* ["insert into t1 (k) values (?)" (DSL/val 3)])
  (-> (sc/fetch *ctx* ["select * from t1"])
      (println)))

;; (deftest json-select
;;   (let [q (dsl/select (.as (json {:a 1}) "dd"))]
;;     (-> (sc/fetch *ctx* q)
;;         (println))))


;; (deftest inserting-arrays
;;   (sc/execute *ctx* "create table t1 (data int[])")
;;   (let [data [1 2 3]]
;;     (sc/execute *ctx* ["insert into t1 (data) values (?)" data])
;;   (let [data (into-array Integer (map int [1 2 3]))]
;;     (sc/execute *ctx* ["insert into t1 (data) values (?::int[])" "{1,2,3}"])
;;     (-> (sc/fetch *ctx* "select * from t1")
;;         (println))))
