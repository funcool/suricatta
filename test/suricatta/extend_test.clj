(ns suricatta.extend-test
  (:require [clojure.test :refer :all]
            [suricatta.core :as sc]
            [suricatta.dsl :as dsl]
            [suricatta.proto :as proto]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [cheshire.core :refer :all])
  (:import org.postgresql.util.PGobject
           org.jooq.RenderContext
           org.jooq.BindContext
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

;; (deftest inserting-json
;;   (sc/execute *ctx* "create table t1 (data json, k int)")
;;   (sc/execute *ctx* ["insert into t1 (data, k) values (?, ?)" (DSL/val #{:foo "bar"} datatype) 3])
;;   (-> (sc/fetch *ctx* ["select * from t1"])
;;       (println)))

(deftype MyJson [data])

(defn myjson
  [data]
  (MyJson. data))

(extend-protocol proto/IParamType
  MyJson
  (render [self]
    (str "'"
         (generate-string (.-data self))
         "'::json"))
  (bind [self stmt index]
    (let [obj (doto (PGobject.)
                (.setType "json")
                (.setValue (generate-string (.-data self))))]
      (.setObject stmt index obj))))

(deftest inserting-json
  (sc/execute *ctx* "create table t1 (k json)")
  (sc/execute *ctx* ["insert into t1 (k) values (?)" (myjson {:foo 1})])
  (let [result (sc/fetch *ctx* ["select * from t1"])
        result1 (first result)]
    (is (= (.getValue (:k result1)) (generate-string {:foo 1})))))

(deftest render-json
  (let [q (-> (dsl/insert-into :t1)
              (dsl/insert-values {:data (myjson {:foo 1})}))]
    (is (= (fmt/get-sql q {:dialect :pgsql :type :inlined})
           "insert into t1 (data) values ('{\"foo\":1}'::json)"))))

;; (deftest inserting-json-dsl
;;   (sc/execute *ctx* "create table t1 (data json)")
;;   (let [q (-> (dsl/insert-into :t1)
;;               (dsl/insert-values {:data (json {:foo 1})})
;;               (dsl/insert-values {:data (json {:bar 2})}))]
;;     (sc/execute *ctx* q))
;;   (-> (sc/fetch *ctx* "select * from t1")
;;       (println)))



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
