(ns suricatta.extend-test
  (:require [clojure.test :refer :all]
            [suricatta.core :as sc]
            [suricatta.impl :as impl]
            [suricatta.dsl :as dsl]
            [suricatta.proto :as proto]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [cheshire.core :as json])
  (:import org.postgresql.util.PGobject
           org.jooq.RenderContext
           org.jooq.BindContext
           org.jooq.impl.DSL))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection setup
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def dbspec {:subprotocol "postgresql"
             :subname "//127.0.0.1/test"})

(def ^:dynamic *ctx*)

(defn setup-connection-fixture
  [end]
  (with-open [ctx (sc/context dbspec)]
    (sc/atomic ctx
      (binding [*ctx* ctx]
        (end)
        (sc/set-rollback! ctx)))))

(use-fixtures :each setup-connection-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests Data
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype MyJson [data])

(defn myjson
  [data]
  (MyJson. data))

(extend-protocol proto/IParamType
  MyJson
  (-render [self ctx]
    (if (impl/inline? ctx)
      (str "'" (json/encode (.-data self)) "'::json")
      "?::json"))

  (-bind [self ctx]
    (when-not (impl/inline? ctx)
      (let [stmt (.statement ctx)
            idx  (.nextIndex ctx)
            obj (doto (PGobject.)
                  (.setType "json")
                  (.setValue (json/encode (.-data self))))]
        (.setObject stmt idx obj)))))

(extend-protocol proto/ISQLType
  PGobject
  (-convert [self]
    (let [type (.getType self)]
      (condp = type
        "json" (json/decode (.getValue self) true)))))

(deftype MyArray [data])

(defn myintarray
  [data]
  (MyArray. data))

(extend-protocol proto/IParamType
  MyArray
  (-render [self ctx]
    (if (impl/inline? ctx)
      (let [items (->> (map str (.-data self))
                       (interpose ","))]
        (str "'{" (apply str items) "}'::bigint[]"))
      "?::bigint[]"))
  (-bind [self ctx]
    (when-not (impl/inline? ctx)
      (let [stmt (.statement ^BindContext ctx)
            idx  (.nextIndex ^BindContext ctx)
            con (.getConnection stmt)
            arr (into-array Long (.-data self))
            arr (.createArrayOf con "bigint" arr)]
        (.setArray stmt idx arr)))))

(extend-protocol proto/ISQLType
  (Class/forName "[Ljava.lang.Long;")
  (-convert [self]
    (into [] self))

  java.sql.Array
  (-convert [self]
    (into [] (.getArray self))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Tests Code
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest inserting-json-test
  (sc/execute *ctx* "create table t1 (k json)")
  (sc/execute *ctx* ["insert into t1 (k) values (?)" (myjson {:foo 1})])

  (let [result (sc/fetch *ctx* ["select * from t1"])
        result1 (first result)]
    (is (= (:k result1) {:foo 1}))))

(deftest inserting-json-using-dsl-test
  (sc/execute *ctx* "create table t1 (k json)")

  (let [q (-> (dsl/insert-into :t1)
              (dsl/insert-values {:k (myjson {:foo 1})}))]
    (sc/execute *ctx* q))

  (let [result (sc/fetch-one *ctx* ["select * from t1"])]
    (is (= (:k result) {:foo 1}))))

(deftest extract-bind-values-test
  (let [d (myjson {:foo 1})
        q (-> (dsl/insert-into :table)
              (dsl/insert-values {:data d}))
        r (fmt/get-bind-values q)]
    (is (= (count r) 1))
    (is (= (.data d) (.data (first r))))))

(deftest render-json-test
  (let [q (-> (dsl/insert-into :t1)
              (dsl/insert-values {:data (myjson {:foo 1})}))]

    (is (= (fmt/get-sql q {:dialect :pgsql :type :indexed})
           "insert into t1 (data) values (?::json)"))

    (is (= (fmt/get-sql q {:dialect :pgsql :type :inlined})
           "insert into t1 (data) values ('{\"foo\":1}'::json)"))))

(deftest inserting-arrays-test
  (sc/execute *ctx* "create table t1 (data bigint[])")
  (let [data (myintarray [1 2 3])]
    (sc/execute *ctx* ["insert into t1 (data) values (?)" data]))
  (let [result (sc/fetch *ctx* "select * from t1")]
    (is (= result [{:data [1 2 3]}]))))

(deftest render-array-test
  (let [q (-> (dsl/insert-into :t1)
              (dsl/insert-values {:data (myintarray [1 2 3])}))]
    (is (= (fmt/get-sql q {:dialect :pgsql :type :inlined})
           "insert into t1 (data) values ('{1,2,3}'::bigint[])"))))
