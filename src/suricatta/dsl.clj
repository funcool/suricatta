(ns suricatta.dsl
  "Sql building dsl"
  (:refer-clojure :exclude [val group-by and or not name])
  (:require [suricatta.core :as core]
            [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocols for constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IField
  (field* [_] "Field constructor"))

(defprotocol ISortField
  (sort-field* [_] "Sort field constructor"))

(defprotocol ITable
  (table* [_] "Table constructor"))

(defprotocol IName
  (name [_] "Name constructor"))

(defprotocol IOnStep
  (on [_ conditions]))

(defprotocol ICondition
  (condition* [_] "Condition constructor"))

(defprotocol IVal
  (val [_] "Val constructor"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocol Implementations
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol IField
  java.lang.String
  (field* [s] (DSL/field s))

  clojure.lang.Keyword
  (field* [kw] (field* (clojure.core/name kw)))

  org.jooq.Field
  (field* [f] f)

  org.jooq.impl.Val
  (field* [v] v))

(extend-protocol ISortField
  java.lang.String
  (sort-field* [s]
    (-> (DSL/field s)
        (.asc)))

  clojure.lang.Keyword
  (sort-field* [kw] (sort-field* (clojure.core/name kw)))

  org.jooq.Field
  (sort-field* [f]
    (.asc f))

  org.jooq.SortField
  (sort-field* [v] v)

  clojure.lang.PersistentVector
  (sort-field* [v]
    (let [field (field* (first v))]
      (case (second v)
        :asc (.asc field)
        :desc (.desc field)))))

(extend-protocol ITable
  java.lang.String
  (table* [s] (DSL/table s))

  clojure.lang.Keyword
  (table* [kw] (table* (clojure.core/name kw)))

  org.jooq.Table
  (table* [t] t))

(extend-protocol IName
  java.lang.String
  (name [s]
    (-> (into-array String [s])
        (DSL/name)))

  clojure.lang.Keyword
  (name [kw] (name (clojure.core/name kw))))

(extend-protocol ICondition
  java.lang.String
  (condition* [s] (DSL/condition s))

  org.jooq.impl.CombinedCondition
  (condition* [c] c)

  org.jooq.impl.SQLCondition
  (condition* [c] c)

  clojure.lang.PersistentVector
  (condition* [v]
    (let [sql    (first v)
          params (rest v)]
      (->> (into-array Object params)
           (DSL/condition sql)))))

(extend-protocol IVal
  Object
  (val [v] (DSL/val v)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DSL functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn field
  [data & {:keys [alias] :as opts}]
  (let [f (field* data)]
    (if alias
      (.as f (clojure.core/name alias))
      f)))

(defn table
  [data & {:keys [alias] :as opts}]
  (let [f (table* data)]
    (if alias
      (.as f (clojure.core/name alias))
      f)))

(defn select
  "Start select statement."
  [& fields]
  (cond
   (instance? org.jooq.WithStep (first fields))
   (.select (first fields)
            (->> (map field (rest fields))
                 (into-array org.jooq.Field)))
   :else
   (->> (map field fields)
        (into-array org.jooq.Field)
        (DSL/select))))

(defn select-distinct
  "Start select statement."
  [& fields]
  (->> (map field fields)
       (into-array org.jooq.Field)
       (DSL/selectDistinct)))

(defn select-from
  "Helper for create select * from <table>
  statement directly (without specify fields)"
  [table']
  (-> (table table')
      (DSL/selectFrom)))

(defn select-count
  []
  (DSL/selectCount))

(defn select-one
  []
  (DSL/selectOne))

(defn select-zero
  []
  (DSL/selectZero))

(defn from
  "Creates from clause."
  [q & tables]
  (->> (map table tables)
       (into-array org.jooq.Table)
       (.from q)))

(defn join
  "Create join clause."
  [q t]
  (.join q t))

(defn left-outer-join
  [q t]
  (.leftOuterJoin q t))

(defn on
  [q & clauses]
  (->> (map condition* clauses)
       (into-array org.jooq.Condition)
       (.on q)))

(defn where
  "Create where clause with variable number
  of conditions (that are implicitly combined
  with `and` logical operator)."
  [q & clauses]
  (->> (map condition* clauses)
       (into-array org.jooq.Condition)
       (.where q)))

(defn exists
  "Create an exists condition."
  [select']
  (DSL/exists select'))

(defn group-by
  [q & fields]
  (->> (map field* fields)
       (into-array org.jooq.GroupField)
       (.groupBy q)))

(defn having
  "Create having clause with variable number
  of conditions (that are implicitly combined
  with `and` logical operator)."
  [q & clauses]
  (->> (map condition* clauses)
       (into-array org.jooq.Condition)
       (.having q)))

(defn order-by
  [q & clauses]
  (->> (map sort-field* clauses)
       (into-array org.jooq.SortField)
       (.orderBy q)))

(defn limit
  "Creates limit clause."
  [q num]
  (.limit q num))

(defn offset
  "Creates offset clause."
  [q num]
  (.offset q num))

(defn as
  "Set alias."
  [q name]
  (.as q name))

(defn and
  "Logican operator `and`."
  [& conditions]
  (let [conditions (map condition* conditions)]
    (reduce (fn [acc v] (.and acc v))
            (first conditions)
            (rest conditions))))

(defn or
  "Logican operator `or`."
  [& conditions]
  (let [conditions (map condition* conditions)]
    (reduce (fn [acc v] (.or acc v))
            (first conditions)
            (rest conditions))))

(defn not
  "Negate a condition."
  [c]
  (DSL/not c))

(defn with
  "Create a WITH clause"
  [& tables]
  (->> (into-array org.jooq.CommonTableExpression tables)
       (DSL/with)))

(defn with-fields
  "Add a list of fields to this name to make this name a DerivedColumnList."
  [n & fields]
  (let [fields' (->> (map clojure.core/name fields)
                     (into-array String))]
    (.fields n fields')))
