(ns suricatta.extend-test
  (:require [clojure.test :refer :all]
            [suricatta.core :as sc]
            [suricatta.impl :as impl]
            [suricatta.proto :as proto]
            [cheshire.core :as json])
  (:import org.postgresql.util.PGobject
           org.jooq.RenderContext
           org.jooq.BindContext
           org.jooq.QueryPart
           org.jooq.impl.DSL))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection setup
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def dbspec {:subprotocol "postgresql"
             :subname "//127.0.0.1:5433/test"
             :user "test"
             :password "test"})

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

(extend-protocol proto/IParam
  MyJson
  (-param [self ctx]
    (let [qp (json/encode (.-data self))]
      (impl/sql->param "{0}::json" qp))))

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

(extend-protocol proto/IParam
  MyArray
  (-param [self ctx]
    (let [items (->> (map str (.-data self))
                     (interpose ","))]
      (impl/sql->param (str "'{" (apply str items) "}'::bigint[]")))))


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

(deftest inserting-arrays-test
  (sc/execute *ctx* "create table t1 (data bigint[])")
  (let [data (myintarray [1 2 3])]
    (sc/execute *ctx* ["insert into t1 (data) values (?)" data]))
  (let [result (sc/fetch *ctx* "select * from t1")]
    (is (= result [{:data [1 2 3]}]))))
