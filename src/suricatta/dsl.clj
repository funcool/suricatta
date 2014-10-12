(ns suricatta.dsl
  "Sql building dsl"
  (:refer-clojure :exclude [val group-by and or not name])
  (:require [suricatta.core :as core]
            [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration
           org.jooq.util.postgres.PostgresDataType))

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

(defprotocol IAlias
  (as* [_ params] "Alias constructor"))

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
  (field* ^org.jooq.Field [^String s]
    (DSL/field s))

  clojure.lang.Keyword
  (field* ^org.jooq.Field [kw]
    (field* (clojure.core/name kw)))

  org.jooq.Field
  (field* ^org.jooq.Field [f] f)

  org.jooq.impl.Val
  (field* ^org.jooq.Field [v] v))

(extend-protocol ISortField
  java.lang.String
  (sort-field* ^org.jooq.SortField [s]
    (-> (DSL/field s)
        (.asc)))

  clojure.lang.Keyword
  (sort-field* ^org.jooq.SortField [kw]
    (sort-field* (clojure.core/name kw)))

  org.jooq.Field
  (sort-field* ^org.jooq.SortField [f] (.asc f))

  org.jooq.SortField
  (sort-field* ^org.jooq.SortField [v] v)

  clojure.lang.PersistentVector
  (sort-field* ^org.jooq.SortField [v]
    (let [field (field* (first v))
          field (case (second v)
                  :asc (.asc field)
                  :desc (.desc field))]
      (if (= (count v) 3)
        (case (first (drop 2 v))
          :nulls-last (.nullsLast field)
          :nulls-first (.nullsFirst field))
        field))))

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

(extend-protocol IAlias
  org.jooq.Name
  (as* [n args]
    (.as n (first args)))

  org.jooq.DerivedColumnList
  (as* [n args]
    (.as n (first args)))

  org.jooq.Table
  (as* [n args]
    (let [^String alias (first args)]
      (->> (into-array String (rest args))
           (.as n alias)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common DSL functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn as
  [o & args]
  (as* o args))

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

(defn for-update
  [q & fields]
  (let [q (.forUpdate q)]
    (if (seq fields)
      (->> (map field* fields)
           (into-array org.jooq.Field)
           (.of q))
      q)))

(defn limit
  "Creates limit clause."
  [q num]
  (.limit q num))

(defn offset
  "Creates offset clause."
  [q num]
  (.offset q num))

(defn union
  [& clauses]
  (reduce (fn [acc v] (.union acc v))
          (first clauses)
          (rest clauses)))

(defn union-all
  [& clauses]
  (reduce (fn [acc v] (.unionAll acc v))
          (first clauses)
          (rest clauses)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Logical operators (for conditions)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common Table Expresions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defmacro row
  [& values]
  `(DSL/row ~@(map (fn [x#] `(field* (val ~x#))) values)))

(defn values
  [& rows]
  (->> (into-array org.jooq.RowN rows)
       (DSL/values)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DDL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^{:doc "Datatypes translation map" :dynamic true}
  *datatypes*
  {:pg/varchar PostgresDataType/VARCHAR})

(defn truncate
  [t]
  (-> (table* t)
      (DSL/truncate)))

(defn alter-table
  [t]
  (-> (table* t)
      (DSL/alterTable)))

(defn- datatype-transformer
  [opts ^org.jooq.DataType acc attr]
  (case attr
    :length (.length acc (attr opts))
    :null   (.nullable acc (attr opts))))

(defn set-column-type
  [^org.jooq.AlterTableStep t ^String name datatype & [opts]]
  (let [^org.jooq.AlterTableFinalStep t (.alter t (field* name))]
    (->> (reduce (partial datatype-transformer opts)
                 (datatype *datatypes*)
                 (keys opts))
         (.set t))))

(defn add-column
  "Add column to alter table step."
  [^org.jooq.AlterTableStep t name datatype & [opts]]
  (->> (reduce (partial datatype-transformer opts)
               (datatype *datatypes*)
               (keys opts))
       (.add t (field* name))))

(defn drop-column
  "Drop column from alter table step."
  [^org.jooq.AlterTableStep t name & [type]]
  (let [^org.jooq.AlterTableDropStep t (.drop t (field* name))]
    (case type
      :cascade (.cascade t)
      :restrict (.restrict t)
      t)))

(defn drop-table
  "Drop table statement constructor."
  [t]
  (-> (table* t)
      (DSL/dropTable)))
