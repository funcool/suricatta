(ns suricatta.extend-test
  (:require [clojure.test :refer :all]
            [suricatta.core :as sc]
            [suricatta.dsl :as dsl]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [cheshire.core :refer :all])
  (:import org.postgresql.util.PGobject))

(def dbspec {:subprotocol "postgresql"
             :subname "//localhost/test"})

(def ^:dynamic *ctx*)

(defn my-fixture
  [end]
  (with-open [ctx (sc/context dbspec)]
    (sc/with-atomic ctx
      (binding [*ctx* ctx]
        (end)
        (sc/set-rollback! ctx)))))

(use-fixtures :each my-fixture)

(defn jsonfield
  [data]
  (proxy [org.jooq.impl.CustomField] [nil, org.jooq.util.postgres.PostgresDataType/JSON]
    (toSQL [^org.jooq.RenderContext rctx]
      (doto rctx
        (.sql "'")
        (.sql (generate-string data))
        (.sql "'::json")))

    (bind [^org.jooq.BindContext bctx]
      (let [obj (doto (PGobject.)
                  (.setType "json")
                  (.setValue (generate-string data)))
            statement (.statement bctx)]
        (.setObject statement
                    (.nextIndex bctx)
                    obj)))))

(deftest inserting-json-fields
  (sc/execute *ctx* "create table t1 (data json)")
  (sc/execute *ctx* ["insert into t1 (data) values (?)" (jsonfield {:foo "bar"})])
  (-> (sc/fetch *ctx* "select * from t1")
      (println)))

(deftest json-fields-in-dsl
  (let [q (-> (dsl/select (jsonfield {:a 1}))
              (dsl/from "dual"))]
    (println (get-sql q))))
