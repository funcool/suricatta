(ns suricatta.dsl-test
  (:require [clojure.test :refer :all]
            [suricatta.core :as sc]
            [suricatta.dsl.alpha :as dsl]))

(deftest select-statement-1
  (let [qq (-> (dsl/select)
               (dsl/from "posts" "p")
               (dsl/join "authors" "a" "p.author_id = a.id")
               (dsl/field "p.*")
               (dsl/field "a.slug" "author_slug")
               (dsl/limit 10))]
    (is (= (dsl/fmt qq)
           ["SELECT p.*, a.slug author_slug FROM posts p INNER JOIN authors a ON (p.author_id = a.id) "]))))

(deftest select-statement-2
  (let [qq (-> (dsl/select)
               (dsl/from "posts" "p")
               (dsl/field "p.id" "post_id")
               (dsl/where "p.category = ?" "offtopic"))]
    (is (= (dsl/fmt qq)
           ["SELECT p.id post_id FROM posts p  WHERE (p.category = ?)" "offtopic"]))))

(deftest update-statement-1
  (let [qq (-> (dsl/update "users" "u")
               (dsl/set "u.username" "foobar")
               (dsl/set "u.email" "foo@bar.com")
               (dsl/where "u.id = ? AND u.deleted_at IS null" 555))]
    (is (= (dsl/fmt qq)
           ["UPDATE users u SET u.username = ?, u.email = ? WHERE (u.id = ? AND u.deleted_at IS null)" "foobar" "foo@bar.com" 555]))))
