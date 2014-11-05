(ns suricatta.core-test
  (:require [clojure.test :refer :all]
            [suricatta.core :refer :all]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [jdbc.core :as jdbc]))

(def dbspec {:subprotocol "h2"
             :subname "mem:"})

(deftest query-execute
  (with-open [conn (jdbc/make-connection dbspec)]
    (jdbc/execute! conn "CREATE TABLE foo (n int)")
    (let [ctx (context conn)]
      (testing "Execute string directly"
        (let [r (execute ctx "insert into foo (n) values (1), (2)")]
          (is (= r 2))))

      (testing "Execute sqlvec directly"
        (let [r (execute ctx ["insert into foo (n) values (?), (?)" 1 2])]
          (is (= r 2)))))))

(deftest query-fetch
  (with-open [ctx (context dbspec)]
    (testing "Fetch by default vector of records."
      (let [sql "select x from system_range(1, 3)"
            r   (fetch ctx sql)]
        (is (= r [{:x 1} {:x 2} {:x 3}]))))

    (testing "Fetch vector of rows"
      (let [sql    "select x, x+1 as i from system_range(1, 3)"
            result (fetch ctx sql {:rows true})]
        (is (= result [[1 2] [2 3] [3 4]]))))

    (testing "Reuse the statement"
      (with-open [q (query ctx ["select ? \"x\" from dual" 1])]
        (is (= (fetch q) [{:x 1}]))
        (is (= (fetch q) [{:x 1}]))
        (is (= (execute q) 1))
        (is (= (execute q) 1))))
))

(deftest transactions
  (testing "Execute in a transaction"
    (with-open [ctx (context dbspec)]
      (execute ctx "create table foo (id int)")
      (with-atomic ctx
        (execute ctx ["insert into foo (id) values (?), (?)" 1 2])
        (try
          (with-atomic ctx
            (execute ctx ["insert into foo (id) values (?), (?)" 3 4])
            (let [result (fetch ctx "select * from foo")]
              (is (= 4 (count result))))
            (throw (RuntimeException. "test")))
          (catch RuntimeException e
            (let [result (fetch ctx "select * from foo")]
              (is (= 2 (count result)))))))))

  (testing "Execute in a transaction with explicit rollback"
    (with-open [ctx (context dbspec)]
      (execute ctx "create table foo (id int)")
      (with-atomic ctx
        (execute ctx ["insert into foo (id) values (?), (?)" 1 2])
        (with-atomic ctx
          (execute ctx ["insert into foo (id) values (?), (?)" 3 4])
          (let [result (fetch ctx "select * from foo")]
            (is (= 4 (count result))))
          (set-rollback! ctx))
        (let [result (fetch ctx "select * from foo")]
          (is (= 2 (count result)))))))

  (testing "Execute in a transaction with explicit rollback"
    (with-open [ctx (context dbspec)]
      (execute ctx "create table foo (id int)")
      (with-atomic ctx
        (execute ctx ["insert into foo (id) values (?), (?)" 1 2])
        (with-atomic ctx
          (execute ctx ["insert into foo (id) values (?), (?)" 3 4])
          (let [result (fetch ctx "select * from foo")]
            (is (= 4 (count result)))))
        (set-rollback! ctx))
      (let [result (fetch ctx "select * from foo")]
        (is (= 0 (count result))))))
)

(deftest lazy-fetch
  (with-open [ctx (context dbspec)]
    (testing "Fetch by default vector of rows."
      (with-atomic ctx
        (with-open [cursor (fetch-lazy ctx "select x from system_range(1, 300)")]
          (let [res (take 3 (cursor->lazyseq cursor {:rows true}))]
            (is (= (mapcat identity (vec res)) [1 2 3]))))))

    (testing "Fetch by default vector of records."
      (with-atomic ctx
        (with-open [cursor (fetch-lazy ctx "select x from system_range(1, 300)")]
          (let [res (take 3 (cursor->lazyseq cursor))]
            (is (= (vec res) [{:x 1} {:x 2} {:x 3}]))))))
))
