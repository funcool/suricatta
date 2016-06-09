(ns suricatta.dsl-test
  (:require [clojure.test :refer :all]
            [suricatta.core :refer :all]
            [suricatta.dsl :as dsl]
            [suricatta.dsl.pgsql :as pgsql]
            [suricatta.format :as fmt]))

(def dbspec {:subprotocol "h2"
             :subname "mem:"})

(deftest rendering-dialect
  (testing "Default dialect."
    (let [q (dsl/select :id :name)]
      (is (= (fmt/sql q) "select id, name"))))

  (testing "Specify concrete dialect"
    (let [q (dsl/select :id :name)]
      (is (= (fmt/sql q {:dialect :pgsql})
             "select id, name")))))

(deftest dsl-fetch-and-execute
  (testing "Fetch using query builded with dsl"
    (with-open [ctx (context dbspec)]
      (let [q   (dsl/select-one)]
        (is (= (fetch ctx q)
               [{:one 1}])))))

  (testing "Execute using query builded with dsl"
    (with-open [ctx (context dbspec)]
      (execute ctx "create table foo (id integer)")
      (let [r (execute ctx (dsl/truncate :foo))]
        (is (= r 0))))))

(deftest dsl-select-clause
  (testing "Basic select clause"
    (let [q   (-> (dsl/select :id :name)
                  (dsl/from :books)
                  (dsl/where ["books.id = ?" 2]))
          sql (fmt/sql q)
          bv  (fmt/get-bind-values q)]
      (is (= sql "select id, name from books where (books.id = ?)"))
      (is (= bv [2]))))

  (testing "Select clause with field as condition and alias"
    (let [q (-> (dsl/select '("foo > 5" "bar"))
                (dsl/from "baz"))]
      (is (= (fmt/sql q)
             "select foo > 5 \"bar\" from baz"))))

  (testing "Select clause with count(*) expresion"
    (let [q (-> (dsl/select '("count(*)" "count"))
                (dsl/from "baz"))]
      (is (= (fmt/sql q)
             "select count(*) \"count\" from baz"))))

  (testing "Select with two tables in from clause 2"
    (let [q (-> (dsl/select-one)
                (dsl/from '("table1" "foo")
                          '("table2" "bar")))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from table1 \"foo\", table2 \"bar\""))))

  (testing "Select clause with join"
    (let [q (-> (dsl/select-one)
                (dsl/from "book")
                (dsl/join "author")
                (dsl/on "book.authorid = book.id"))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book join author on (book.authorid = book.id)"))))

  (testing "Select clause with join and aliases"
    (let [q (-> (dsl/select-one)
                (dsl/from '("book" "b"))
                (dsl/join '("author" "a"))
                (dsl/on "b.authorid = a.id"))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book \"b\" join author \"a\" on (b.authorid = a.id)"))))

  (testing "Select clause with join on table"
    (let [q (-> (dsl/select-one)
                (dsl/from (-> (dsl/table "book")
                              (dsl/join "author")
                              (dsl/on "book.authorid = book.id"))))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book join author on (book.authorid = book.id)"))))

  (testing "Select clause with where"
    (let [q (-> (dsl/select-one)
                (dsl/from "book")
                (dsl/where ["book.age > ?" 100]
                           ["book.in_store is ?", true]))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book where ((book.age > ?) and (book.in_store is ?))"))))

  (testing "Where with nil argument"
    (let [q (-> (dsl/select "name")
                (dsl/from "book")
                (dsl/where ["author_id = coalesce(?, 123)" nil]))]
      (is (= (fmt/sql q)
             "select name from book where (author_id = coalesce(?, 123))"))))

  (testing "Select clause with group by"
    (let [q (-> (dsl/select (dsl/field "authorid")
                            (dsl/field "count(*)"))
                (dsl/from "book")
                (dsl/group-by (dsl/field "authorid")))]
      (is (= (fmt/sql q)
             "select authorid, count(*) from book group by authorid"))))

  (testing "Select clause with group by with having"
    (let [q (-> (dsl/select (dsl/field "authorid")
                            (dsl/field "count(*)"))
                (dsl/from "book")
                (dsl/group-by (dsl/field "authorid"))
                (dsl/having ["count(*) > ?", 2]))]
      (is (= (fmt/sql q)
             "select authorid, count(*) from book group by authorid having (count(*) > ?)"))))

  (testing "Select clause with order by without explicit order"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by :name))]
      (is (= (fmt/sql q)
             "select name from book order by name asc"))))

  (testing "Select clause with order by with explicit order"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by [:name :desc]))]
      (is (= (fmt/sql q)
             "select name from book order by name desc"))))

  (testing "Select clause with order by with explicit order by index"
    (let [q (-> (dsl/select :id :name)
                (dsl/from "book")
                (dsl/order-by ["1" :desc]
                              ["2" :asc]))]
      (is (= (fmt/sql q)
             "select id, name from book order by 1 desc, 2 asc"))))

  (testing "Select clause with order by with explicit order with nulls"
    (let [q (-> (dsl/select :name)
                (dsl/from "book")
                (dsl/order-by [:name :desc :nulls-last]))]
      (is (= (fmt/sql q)
             "select name from book order by name desc nulls last"))))

  (testing "select with limit and offset"
    (let [q (-> (dsl/select :name)
                (dsl/from :book)
                (dsl/limit 10)
                (dsl/offset 100))]
      (is (= (fmt/sql q)
             "select name from book limit ? offset ?"))))

  (testing "select with for update without fields"
    (let [q (-> (dsl/select :name)
                (dsl/from :book)
                (dsl/for-update))]
      (is (= (fmt/sql q)
             "select name from book for update"))))

  (testing "select with for update with fields"
    (let [q (-> (dsl/select :name)
                (dsl/from :book)
                (dsl/for-update :name))]
      (is (= (fmt/sql q)
             "select name from book for update of name"))))

  (testing "union two selects"
    (let [q (dsl/union
             (-> (dsl/select :name)
                 (dsl/from :books))
             (-> (dsl/select :name)
                 (dsl/from :articles)))]
      (is (= (fmt/sql q)
             "(select name from books) union (select name from articles)"))))

  (testing "union all two selects"
    (let [q (dsl/union-all
             (-> (dsl/select :name)
                 (dsl/from :books))
             (-> (dsl/select :name)
                 (dsl/from :articles)))]
      (is (= (fmt/sql q)
             "(select name from books) union all (select name from articles)"))))
)

(deftest dsl-join
  (testing "cross-join"
    (let [q (-> (dsl/select-one)
                (dsl/from :book)
                (dsl/cross-join :article))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book cross join article"))))

  (testing "dsl-full-outer-join"
    (let [q (-> (dsl/select-one)
                (dsl/from :book)
                (dsl/full-outer-join :article)
                (dsl/on "article.id = book.id"))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book full outer join article on (article.id = book.id)"))))

  (testing "dsl-left-outer-join"
    (let [q (-> (dsl/select-one)
                (dsl/from :book)
                (dsl/left-outer-join :article)
                (dsl/on "article.id = book.id"))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book left outer join article on (article.id = book.id)"))))

  (testing "dsl-right-outer-join"
    (let [q (-> (dsl/select-one)
                (dsl/from :book)
                (dsl/right-outer-join :article)
                (dsl/on "article.id = book.id"))]
      (is (= (fmt/sql q)
             "select 1 \"one\" from book right outer join article on (article.id = book.id)"))))
)

(deftest dsl-table-expressions
  (testing "Values table expression"
    (let [q (-> (dsl/select :f1 :f2)
                (dsl/from
                 (-> (dsl/values
                      (dsl/row 1 2)
                      (dsl/row 3 4))
                     (dsl/to-table "t1" "f1" "f2"))))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "select f1, f2 from (values(?, ?), (?, ?)) as \"t1\"(\"f1\", \"f2\")"))))

  (testing "Nested select in condition clause"
    (let [q (-> (dsl/select)
                (dsl/from :book)
                (dsl/where ["book.age = ({0})" (dsl/select-one)]))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "select * from book where (book.age = (select 1 as \"one\"))"))))

  (testing "Nested select in from clause"
    (let [q (-> (dsl/select)
                (dsl/from (-> (dsl/select :f1)
                              (dsl/from :t1)
                              (dsl/to-table "tt1" "f1"))))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "select \"tt1\".\"f1\" from (select f1 from t1) as \"tt1\"(\"f1\")"))))

  (testing "Nested select in select fields"
    (let [sq (-> (dsl/select (dsl/field "count(*)"))
                 (dsl/from :book)
                 (dsl/where "book.authorid = author.id"))
          q  (-> (dsl/select :fullname, (dsl/field sq "books"))
                 (dsl/from :author))]
      (is (= (fmt/sql q)
             "select fullname, (select count(*) from book where (book.authorid = author.id)) \"books\" from author"))))

  (testing "Nested select returned as array"
    (let [sq (-> (dsl/select (dsl/field :id))
                 (dsl/from :book)
                 (dsl/where "book.authorid = author.id"))
          q  (-> (dsl/select :fullname, (dsl/field (pgsql/array sq) "books"))
                 (dsl/from :author))]
      (is (= (fmt/sql q)
             "select fullname, array(select id from book where (book.authorid = author.id)) \"books\" from author"))))

  (testing "Nested select in where clause using exists"
    (let [q  (-> (dsl/select :fullname)
                 (dsl/from :author)
                 (dsl/where (dsl/exists (-> (dsl/select :id)
                                            (dsl/from :table)
                                            (dsl/where ["table.author_id = author.id"])))))]
      (is (= (fmt/sql q)
             "select fullname from author where exists (select id from table where (table.author_id = author.id))"))))

  (testing "Nested select in where clause using not-exists"
    (let [q  (-> (dsl/select :fullname)
                 (dsl/from :author)
                 (dsl/where (dsl/not-exists (-> (dsl/select :id)
                                                (dsl/from :table)
                                                (dsl/where ["table.author_id = author.id"])))))]
      (is (= (fmt/sql q)
             "select fullname from author where not exists (select id from table where (table.author_id = author.id))")))))

(deftest dsl-insert
  (testing "Insert statement from values as maps"
    (let [q (-> (dsl/insert-into :t1)
                (dsl/insert-values {:f1 1 :f2 3})
                (dsl/insert-values {:f1 2 :f2 4}))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "insert into t1 (f1, f2) values (?, ?), (?, ?)"))))
  (testing "Insert statement with nil values"
    (let [q (-> (dsl/insert-into :t1)
                (dsl/insert-values {:f1 1 :f2 nil :f3 2}))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "insert into t1 (f1, f3) values (?, ?)"))))
  (testing "Insert statement from values as maps with returning"
    (let [q (-> (dsl/insert-into :t1)
                (dsl/insert-values {:f1 1 :f2 3})
                (dsl/insert-values {:f1 2 :f2 4})
                (dsl/returning :f1))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "insert into t1 (f1, f2) values (?, ?), (?, ?) returning f1")))))

(deftest dsl-update
  (testing "Update statement without condition using map"
    (let [q (-> (dsl/update :t1)
                (dsl/set (sorted-map :id 2 :name "foo")))]
      (is (= (fmt/sql q)
             "update t1 set id = ?, name = ?"))))

  (testing "Update statement without condition"
    (let [q (-> (dsl/update :t1)
                (dsl/set :id 2)
                (dsl/set :name "foo"))]
      (is (= (fmt/sql q)
             "update t1 set id = ?, name = ?"))))

  (testing "Update statement without condition and using function"
    (let [q (-> (dsl/update :t1)
                (dsl/set :val (dsl/f ["concat(val, ?)" "bar"])))]
      (is (= (fmt/sqlvec q)
             ["update t1 set val = concat(val, ?)" "bar"]))))

  (testing "Update statement with condition"
    (let [q (-> (dsl/update :t1)
                (dsl/set :name "foo")
                (dsl/where ["id = ?" 2]))]
      (is (= (fmt/sql q)
             "update t1 set name = ? where (id = ?)"))))

  (testing "Update statement with subquery"
    (let [q (-> (dsl/update :t1)
                (dsl/set :f1 (-> (dsl/select :f2)
                                 (dsl/from :t2)
                                 (dsl/where ["id = ?" 2]))))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "update t1 set f1 = (select f2 from t2 where (id = ?))"))))

  (testing "Update statement with subquery with two fields"
    (let [q (-> (dsl/update :t1)
                (dsl/set (dsl/row (dsl/field :f1)
                                  (dsl/field :f2))
                         (-> (dsl/select :f3 :f4)
                             (dsl/from :t2)
                             (dsl/where ["id = ?" 2]))))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "update t1 set (f1, f2) = (select f3, f4 from t2 where (id = ?))"))))

  (testing "Update statement with returning clause"
    (let [q (-> (dsl/update :t1)
                (dsl/set :f1 2)
                (dsl/returning :id))]
      (is (= (fmt/sql q {:dialect :pgsql})
             "update t1 set f1 = ? returning id"))))
)

(deftest dsl-delete
  (testing "Delete statement"
    (let [q (-> (dsl/delete :t1)
                (dsl/where "id = 1"))]
      (is (= (fmt/sql q)
             "delete from t1 where (id = 1)")))))

(deftest dsl-common-table-expressions
  (testing "Common table expressions"
    (let [
          ;; cte1 (-> (dsl/name :t1)
          ;;          (dsl/with-fields :f1 :f2)
          ;;          (dsl/to-table (dsl/select (dsl/val 1) (dsl/val "a"))))
          ;; cte2 (-> (dsl/name :t2)
          ;;          (dsl/with-fields :f1 :f2)
          ;;          (dsl/to-table (dsl/select (dsl/val 2) (dsl/val "b"))))
          ;; q1   (-> (dsl/with cte1 cte2)
          ;;          (dsl/select (dsl/field "t1.f2"))
          ;;          (dsl/from :t1 :t2))

          ;; Same as previous code but less verbose.
          q    (-> (dsl/with
                    (-> (dsl/name :t1)
                        (dsl/with-fields :f1 :f2)
                        (dsl/to-table (dsl/select (dsl/val 1) (dsl/val "a"))))
                    (-> (dsl/name :t2)
                        (dsl/with-fields :f1 :f2)
                        (dsl/to-table (dsl/select (dsl/val 2) (dsl/val "b")))))
                   (dsl/select (dsl/field "t1.f2"))
                   (dsl/from :t1 :t2))
          sql  (fmt/sql q {:type :inlined :dialect :pgsql})
          esql (str "with \"t1\"(\"f1\", \"f2\") as (select 1, 'a'), "
                    "\"t2\"(\"f1\", \"f2\") as (select 2, 'b') "
                    "select t1.f2 from t1, t2")]
      (is (= sql esql))))
)

(deftest dsl-ddl
  (testing "Truncate table"
    (let [q (dsl/truncate :table1)]
      (is (= (fmt/sql q)
             "truncate table table1"))))

  (testing "Alter table with add column"
    (let [q (-> (dsl/alter-table :t1)
                (dsl/add-column :title {:type :pg/varchar :length 2 :null false}))]
      (is (= (fmt/sql q)
             "alter table t1 add title varchar(2) not null"))))

  (testing "Create table with add column"
    (let [q (-> (dsl/create-table :t1)
                (dsl/add-column :title {:type :pg/varchar :length 2 :null false}))]
      (is (= (fmt/sql q)
             "create table t1(title varchar(2) not null)"))))

  (testing "Alter table with set new datatype"
    (let [q (-> (dsl/alter-table :t1)
                (dsl/alter-column :title {:type :pg/varchar :length 100}))]
      (is (= (fmt/sql q)
             "alter table t1 alter title varchar(100)"))))

  (testing "Alter table with drop column"
    (let [q (-> (dsl/alter-table :t1)
                (dsl/drop-column :title :cascade))]
      (is (= (fmt/sql q)
             "alter table t1 drop title cascade"))))

  (testing "Create index support"
    (let [q (-> (dsl/create-index "test")
                (dsl/on :t1 :title))]
      (is (= (fmt/sql q)
             "create index \"test\" on t1(title)"))))

  (testing "Create index with expressions support"
    (let [q (-> (dsl/create-index "test")
                (dsl/on :t1 (dsl/field "lower(title)")))]
      (is (= (fmt/sql q)
             "create index \"test\" on t1(lower(title))"))))

  (testing "Drop index"
    (let [q (dsl/drop-index :test)]
      (is (= (fmt/sql q)
             "drop index \"test\""))))

  (testing "Create sequence"
    (let [q (dsl/create-sequence "testseq")]
      (is (= (fmt/sql q {:dialect :pgsql})
             "create sequence \"testseq\""))))

  (testing "Alter sequence"
    (let [q (dsl/alter-sequence "testseq" true)]
      (is (= (fmt/sql q {:dialect :pgsql})
             "alter sequence \"testseq\" restart"))))

  (testing "Alter sequence with specific number"
    (let [q (dsl/alter-sequence "testseq" 19)]
      (is (= (fmt/sql q {:dialect :pgsql})
             "alter sequence \"testseq\" restart with 19"))))

  (testing "Drop sequence"
    (let [q (dsl/drop-sequence :test)]
      (is (= (fmt/sql q {:dialect :pgsql})
             "drop sequence \"test\""))))

  (testing "Drop sequence if exists"
    (let [q (dsl/drop-sequence :test true)]
      (is (= (fmt/sql q {:dialect :pgsql})
             "drop sequence if exists \"test\""))))

  (testing "Drop table"
    (let [q (dsl/drop-table :t1)]
      (is (= (fmt/sql q)
             "drop table t1"))))
)
