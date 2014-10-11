(ns suricatta.dsl-test
  (:require [clojure.test :refer :all]
            [suricatta.core :refer :all]
            [suricatta.dsl :as dsl]
            [suricatta.format :as fmt]
            [jdbc.core :as jdbc]))

(def dbspec {:subprotocol "h2"
             :subname "mem:"})

(deftest rendering-dialect
  (testing "Default dialect."
    (let [q (dsl/select :id :name)]
      (is (= (fmt/get-sql q) "select id, name from dual"))))

  (testing "Specify concrete dialect"
    (let [q (dsl/select :id :name)]
      (is (= (fmt/get-sql q {:dialect :pgsql})
             "select id, name"))))

  (testing "Specify dialect with associated config"
    (with-open [conn (jdbc/make-connection dbspec)]
      (let [ctx (context conn)
            q1   (dsl/select :id :name)
            q2   (query ctx q1)]
        (is (= (fmt/get-sql q2 {:dialect :pgsql})
               "select id, name"))
        (is (= (fmt/get-sql q2)
               "select id, name from dual"))))))

(deftest dsl-select-clause
  (testing "Basic select clause"
    (let [q   (-> (dsl/select :id :name)
                  (dsl/from :books)
                  (dsl/where ["books.id = ?" 2]))
          sql (fmt/get-sql q)
          bv  (fmt/get-bind-values q)]
      (is (= sql "select id, name from books where (books.id = ?)"))
      (is (= bv [2]))))

  (testing "Select clause with field as condition and alias"
    (let [q (-> (dsl/select (dsl/field "foo > 5" :alias "bar"))
                (dsl/from "baz"))]
      (is (= (fmt/get-sql q)
             "select foo > 5 \"bar\" from baz"))))

  (testing "Select clause with count(*) expresion"
    (let [q (-> (dsl/select (dsl/field "count(*)" :alias "count"))
                (dsl/from "baz"))]
      (is (= (fmt/get-sql q)
             "select count(*) \"count\" from baz"))))

  (testing "Select with two tables in from clause"
    (let [q (-> (dsl/select-one)
                (dsl/from
                 (dsl/table "table1" :alias "foo")
                 (dsl/table "table2" :alias "bar")))]
      (is (= (fmt/get-sql q)
             "select 1 \"one\" from table1 \"foo\", table2 \"bar\""))))

  (testing "Select clause with join"
    (let [q (-> (dsl/select-one)
                (dsl/from (dsl/table "book"))
                (dsl/join "author")
                (dsl/on "book.authorid = book.id"))]
      (is (= (fmt/get-sql q)
             "select 1 \"one\" from book join author on (book.authorid = book.id)"))))

  (testing "Select clause with join on table"
    (let [q (-> (dsl/select-one)
                (dsl/from (-> (dsl/table "book")
                              (dsl/join "author")
                              (dsl/on "book.authorid = book.id"))))]
      (is (= (fmt/get-sql q)
             "select 1 \"one\" from book join author on (book.authorid = book.id)"))))

  (testing "Select clause with where"
    (let [q (-> (dsl/select-one)
                (dsl/from "book")
                (dsl/where ["book.age > ?" 100]
                           ["book.in_store is ?", true]))]
      (is (= (fmt/get-sql q)
             "select 1 \"one\" from book where ((book.age > ?) and (book.in_store is ?))"))))

  (testing "Select clause with group by"
    (let [q (-> (dsl/select (dsl/field "authorid")
                            (dsl/field "count(*)"))
                (dsl/from "book")
                (dsl/group-by (dsl/field "authorid")))]
      (is (= (fmt/get-sql q)
             "select authorid, count(*) from book group by authorid"))))

  (testing "Select clause with group by with having"
    (let [q (-> (dsl/select (dsl/field "authorid")
                            (dsl/field "count(*)"))
                (dsl/from "book")
                (dsl/group-by (dsl/field "authorid"))
                (dsl/having ["count(*) > ?", 2]))]
      (is (= (fmt/get-sql q)
             "select authorid, count(*) from book group by authorid having (count(*) > ?)"))))

  (testing "Select clause with order by without explicit order"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by :name))]
      (is (= (fmt/get-sql q)
             "select name from book order by name asc"))))

  (testing "Select clause with order by with explicit order"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by [:name :desc]))]
      (is (= (fmt/get-sql q)
             "select name from book order by name desc"))))

  (testing "Select clause with order by with explicit order by index"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by ["1" :desc]
                              ["2" :asc]))]
      (is (= (fmt/get-sql q)
             "select name from book order by 1 desc, 2 asc"))))

  (testing "Select clause with order by with explicit order with nulls"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by [:name :desc :nulls-last]))]
      (is (= (fmt/get-sql q)
             "select name from book order by name desc nulls last"))))

  (testing "select with limit and offset"
    (let [q (-> (dsl/select :name)
                (dsl/from :book)
                (dsl/limit 10)
                (dsl/offset 100))]
      (is (= (fmt/get-sql q)
             "select name from book limit ? offset ?"))))
)

(deftest dsl-common-table-expressions
  (testing "Common table expressions"
    (let [cte1 (-> (dsl/name :t1)
                   (dsl/with-fields :f1 :f2)
                   (dsl/as (dsl/select (dsl/val 1) (dsl/val "a"))))
          cte2 (-> (dsl/name :t2)
                   (dsl/with-fields :f1 :f2)
                   (dsl/as (dsl/select (dsl/val 2) (dsl/val "b"))))
          q1   (-> (dsl/with cte1 cte2)
                   (dsl/select (dsl/field "t1.f2"))
                   (dsl/from :t1 :t2))

          ;; Same as previous code but less verbose.
          q    (-> (dsl/with
                    (-> (dsl/name :t1)
                        (dsl/with-fields :f1 :f2)
                        (dsl/as (dsl/select (dsl/val 1) (dsl/val "a"))))
                    (-> (dsl/name :t2)
                        (dsl/with-fields :f1 :f2)
                        (dsl/as (dsl/select (dsl/val 2) (dsl/val "b")))))
                   (dsl/select (dsl/field "t1.f2"))
                   (dsl/from :t1 :t2))
          sql  (fmt/get-sql q {:type :inlined :dialect :pgsql})
          esql (str "with \"t1\"(\"f1\", \"f2\") as (select 1, 'a'), "
                    "\"t2\"(\"f1\", \"f2\") as (select 2, 'b') "
                    "select t1.f2 from t1, t2")]
      (is (= sql esql))))
)

