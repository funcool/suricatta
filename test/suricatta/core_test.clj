(ns suricatta.core-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :refer [<!!]]
            [suricatta.core :as sc]
            [suricatta.async :as sca]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [jdbc.core :as jdbc]
            [cats.monad.exception :as exc]))

(def dbspec {:subprotocol "h2"
             :subname "mem:"})

(def pgdbspec {:subprotocol "postgresql"
               :subname "//127.0.0.1/test"})

(def ^:dynamic *ctx*)

(defn database-fixture
  [end]
  (with-open [ctx (sc/context pgdbspec)]
    (sc/atomic ctx
      (binding [*ctx* ctx]
        (end)
        (sc/set-rollback! ctx)))))

(use-fixtures :each database-fixture)

(deftest query-execute
  (sc/execute *ctx* "create temporary table foo (n int) on commit drop")

  (testing "Execute string directly"
    (let [r (sc/execute *ctx* "insert into foo (n) values (1), (2)")]
      (is (= r 2))))

  (testing "Execute sqlvec directly"
    (let [r (sc/execute *ctx* ["insert into foo (n) values (?), (?)" 1 2])]
      (is (= r 2)))))

(deftest query-fetch
  (testing "Fetch by default vector of records."
    (let [sql "select x from generate_series(1, 3) as x"
          r   (sc/fetch *ctx* sql)]
      (is (= r [{:x 1} {:x 2} {:x 3}]))))

  (testing "Fetch vector of rows"
    (let [sql    "select x, x+1 as i from generate_series(1, 3) as x"
          result (sc/fetch *ctx* sql {:format :row})]
      (is (= result [[1 2] [2 3] [3 4]]))))

  (testing "Reuse the statement"
    (with-open [q (sc/query *ctx* ["select ? \"x\"" 1])]
      (is (= (sc/fetch q) [{:x 1}]))
      (is (= (sc/fetch q) [{:x 1}]))
      (is (= (sc/execute q) 1))
      (is (= (sc/execute q) 1))))
)

(deftest lazy-fetch
  (testing "Fetch by default vector of rows."
    (sc/atomic *ctx*
            (with-open [cursor (sc/fetch-lazy *ctx* "select x from generate_series(1, 300) as x")]
              (let [res (take 3 (sc/cursor->lazyseq cursor {:rows true}))]
                (is (= (mapcat identity (vec res)) [1 2 3]))))))

  (testing "Fetch by default vector of records."
    (sc/atomic *ctx*
            (with-open [cursor (sc/fetch-lazy *ctx* "select x from generate_series(1, 300) as x")]
              (let [res (take 3 (sc/cursor->lazyseq cursor))]
                (is (= (vec res) [{:x 1} {:x 2} {:x 3}]))))))
)

(deftest fetch-format
  (testing "Fetch in csv format"
    (let [sql "select x, x+1 as i, 'a,b' as k from generate_series(1, 1) as x"
          result (sc/fetch *ctx* sql {:format :csv})]
      (is (= (str "x,i,k\n1,2,\"a,b\"\n") result))))

  (testing "Fetch in json format"
    (let [sql "select x, x+1 as i, 'a,b' as k from generate_series(1, 1) as x"
          result (sc/fetch *ctx* sql {:format :json})]
      (is (= (str "{\"fields\":[{\"name\":\"x\",\"type\":\"INT4\"},"
                  "{\"name\":\"i\",\"type\":\"INT4\"},"
                  "{\"name\":\"k\",\"type\":\"OTHER\"}],"
                  "\"records\":[[1,2,\"a,b\"]]}")
             result))))
)

(deftest async-support
  (sc/execute *ctx* "create table foo (n int)")
  (testing "Execute query asynchronously"
    (let [result (<!! (sca/execute *ctx* "insert into foo (n) values (1), (2)"))]
      (is (= result (exc/success 2)))
      (is (= 1 (deref (.-act *ctx*))))))

  (testing "Fetching query asynchronously"
    (let [ch      (sca/fetch *ctx* "select * from foo order by n")
          result1 (<!! ch)
          result2 (<!! ch)]
      (is (= result1 (exc/success {:n 1})))
      (is (= result2 (exc/success {:n 2})))))
)

(deftest transactions
  (testing "Execute in a transaction"
    (with-open [ctx (sc/context dbspec)]
      (sc/execute ctx "create table foo (id int)")
      (sc/atomic ctx
        (sc/execute ctx ["insert into foo (id) values (?), (?)" 1 2])
        (try
          (sc/atomic ctx
            (sc/execute ctx ["insert into foo (id) values (?), (?)" 3 4])
            (let [result (sc/fetch ctx "select * from foo")]
              (is (= 4 (count result))))
            (throw (RuntimeException. "test")))
          (catch RuntimeException e
            (let [result (sc/fetch ctx "select * from foo")]
              (is (= 2 (count result)))))))))

  (testing "Execute in a transaction with explicit rollback"
    (with-open [ctx (sc/context dbspec)]
      (sc/execute ctx "create table foo (id int)")
      (sc/atomic ctx
        (sc/execute ctx ["insert into foo (id) values (?), (?)" 1 2])
        (sc/atomic ctx
          (sc/execute ctx ["insert into foo (id) values (?), (?)" 3 4])
          (let [result (sc/fetch ctx "select * from foo")]
            (is (= 4 (count result))))
          (sc/set-rollback! ctx))
        (let [result (sc/fetch ctx "select * from foo")]
          (is (= 2 (count result)))))))

  (testing "Execute in a transaction with explicit rollback"
    (with-open [ctx (sc/context dbspec)]
      (sc/execute ctx "create table foo (id int)")
      (sc/atomic ctx
        (sc/execute ctx ["insert into foo (id) values (?), (?)" 1 2])
        (sc/atomic ctx
          (sc/execute ctx ["insert into foo (id) values (?), (?)" 3 4])
          (let [result (sc/fetch ctx "select * from foo")]
            (is (= 4 (count result)))))
        (sc/set-rollback! ctx))
      (let [result (sc/fetch ctx "select * from foo")]
        (is (= 0 (count result))))))
)
