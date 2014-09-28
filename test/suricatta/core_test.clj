(ns suricatta.core-test
  (:require [clojure.test :refer :all]
            [suricatta.core :refer :all]
            [suricatta.format :refer [get-sql get-bind-values sqlvec] :as fmt]
            [jdbc.core :as jdbc]))

(def dbspec {:subprotocol "h2"
             :subname "mem:"})

;; (def dbspec {:subprotocol "postgresql"
;;              :subname "//localhost:5432/test"})

(deftest basic-query-constructors
  (with-open [conn (jdbc/make-connection dbspec)]
    (let [ctx (context conn)]
      (testing "Simple string constructor using query function"
        (let [q (query ctx "select 1")]
          (is (= (fmt/get-sql q) "select 1"))))

      (testing "Simple sqlvec constructor with binds using query function"
        (let [q (query ctx ["select ?" 1])]
          (is (= (fmt/get-bind-values q) [1]))
          ;; query instances always renders inlined
          (is (= (fmt/get-sql q) "select 1"))))

      (testing "Simple string constructor using result-query function"
        (let [q (result-query ctx "select 1")]
          (is (= (fmt/get-sql q) "select 1"))))

      (testing "Simple sqlvec constructor with binds using result-query function"
        (let [q (result-query ctx ["select ?" 1])]
          (is (= (fmt/get-bind-values q) [1]))
          ;; query instances always renders indexed
          (is (= (fmt/get-sql q) "select ?"))))

      (testing "Simple sqlvec constructor and getter"
        (let [q (query ctx ["select ?" 1])]
          (is (= (fmt/sqlvec q) ["select ?" 1]))))
)))

(deftest basic-query-executor
  (with-open [conn (jdbc/make-connection dbspec)]
    (jdbc/execute! conn "CREATE TABLE foo (n int)")
    (let [ctx (context conn)]
      (testing "Execute query instance build from string"
        (let [q (query ctx "insert into foo (n) values (1), (2)")
              r (execute q)]
          (is (= r 2))))

      (testing "Execute query instance build from sqlvec"
        (let [q (query ctx ["insert into foo (n) values (?), (?)" 1 2])
              r (execute q)]
          (is (= r 2))))

      (testing "Execute string directly"
        (let [r (execute ctx "insert into foo (n) values (1), (2)")]
          (is (= r 2))))

      (testing "Execute sqlvec directly"
        (let [r (execute ctx ["insert into foo (n) values (?), (?)" 1 2])]
          (is (= r 2))))
)))

(deftest basic-query-result-fetch
  (with-open [conn (jdbc/make-connection dbspec)]
    ;; (jdbc/execute! conn "CREATE TABLE foo (n int)")
    ;; (let [sql "INSERT INTO foo (n) VALUES (?)"]
    ;;   (execute-prepared! conn sql [1] [2] [3]))
    (let [ctx (context conn)]
      (testing "Simple fetch"
        (let [sql "select * from system_range(1, 3)"
              q   (result-query ctx sql)
              r   (fetch q)]
          (is (= r [{:x 1} {:x 2} {:x 3}]))))
)))
