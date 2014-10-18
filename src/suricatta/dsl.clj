(ns suricatta.dsl
  "Sql building dsl"
  (:refer-clojure :exclude [val group-by and or not name set])
  (:require [suricatta.core :as core]
            [suricatta.types :as types :refer [defer]]
            [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration
           org.jooq.util.postgres.PostgresDataType
           suricatta.types.Context))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocols for constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IField
  (field* [_] "Field constructor"))

(defprotocol ISortField
  (sort-field* [_] "Sort field constructor"))

(defprotocol ITable
  (table* [_] "Table constructor."))

(defprotocol IName
  (name* [_] "Name constructor (mainly used with CTE)"))

(defprotocol ITableAlias
  (as* [_ params] "Table alias constructor"))

(defprotocol IFieldAlias
  (as-field* [_ params] "Field alias constructor"))

(defprotocol IOnStep
  (on [_ conditions]))

(defprotocol ICondition
  (condition* [_] "Condition constructor"))

(defprotocol IVal
  (val* [_] "Val constructor"))

(defprotocol IDeferred
  "Protocol mainly defined for uniform unwrapping
  deferred queries."
  (unwrap* [_] "Unwrap the object"))

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

  ;; Deferred
  ;; (field* ^org.jooq.Field [v] @v))

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
  (table* [t] t)

  org.jooq.TableLike
  (table* [t] (.asTable t))

  suricatta.types.Deferred
  (table* [t] @t))

(extend-protocol IName
  java.lang.String
  (name* [s]
    (-> (into-array String [s])
        (DSL/name)))

  clojure.lang.Keyword
  (name* [kw] (name* (clojure.core/name kw))))

(extend-protocol ICondition
  java.lang.String
  (condition* [s] (DSL/condition s))

  org.jooq.impl.CombinedCondition
  (condition* [c] c)

  org.jooq.impl.SQLCondition
  (condition* [c] c)

  clojure.lang.PersistentList
  (condition* [v]
    (let [sql   (first v)
          parts (rest v)]
      (->> (into-array org.jooq.QueryPart parts)
           (DSL/condition sql))))

  clojure.lang.PersistentVector
  (condition* [v]
    (let [sql    (first v)
          params (rest v)]
      (->> (into-array Object params)
           (DSL/condition sql)))))

(extend-protocol IVal
  Object
  (val* [v] (DSL/val v)))

(extend-protocol IFieldAlias
  org.jooq.FieldLike
  (as-field* [n args]
    (let [^String alias (first args)]
      (.asField n alias)))

  suricatta.types.Deferred
  (as-field* [n args] (as-field* @n args)))

(extend-protocol ITableAlias
  org.jooq.Name
  (as* [n args]
    (.as n (first args)))

  org.jooq.DerivedColumnList
  (as* [n args]
    (.as n (first args)))

  org.jooq.TableLike
  (as* [n args]
    (let [^String alias (first args)]
      (->> (into-array String (rest args))
           (.asTable n alias))))

  suricatta.types.Deferred
  (as* [t args]
    (as* @t args)))

(extend-protocol IDeferred
  Object
  (unwrap* [self] self)

  suricatta.types.Deferred
  (unwrap* [self] @self))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common DSL functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn as
  [o & args]
  (defer
    (->> (map unwrap* args)
         (as* o))))

(defn as-field
  [o & args]
  (defer
    (->> (map unwrap* args)
         (as-field* o))))

(defn field
  [data & {:keys [alias] :as opts}]
  (defer
    (let [f (field* data)]
      (if alias
        (.as f (clojure.core/name alias))
        f))))

(defn val
  [v]
  (val* v))

(defn table
  [data & {:keys [alias] :as opts}]
  (defer
    (let [f (table* data)]
      (if alias
        (.as f (clojure.core/name alias))
        f))))

(defn select
  "Start select statement."
  [& fields]
  (defer
    (let [fields (map unwrap* fields)]
      (cond
       (instance? org.jooq.WithStep (first fields))
       (.select (first fields)
                (->> (map field* (rest fields))
                     (into-array org.jooq.Field)))
       :else
       (->> (map field* fields)
            (into-array org.jooq.Field)
            (DSL/select))))))

(defn select-distinct
  "Start select statement."
  [& fields]
  (defer
    (->> (map (comp field unwrap*) fields)
         (into-array org.jooq.Field)
         (DSL/selectDistinct))))

(defn select-from
  "Helper for create select * from <table>
  statement directly (without specify fields)"
  [table']
  (-> (table* table')
      (DSL/selectFrom)))

(defn select-count
  []
  (DSL/selectCount))

(defn select-one
  []
  (defer (DSL/selectOne)))

(defn select-zero
  []
  (DSL/selectZero))

(defn from
  "Creates from clause."
  [f & tables]
  (defer
    (->> (map table* tables)
         (into-array org.jooq.TableLike)
         (.from @f))))

(defn join
  "Create join clause."
  [q t]
  (defer
    (.join @q t)))

(defn left-outer-join
  [q t]
  (.leftOuterJoin q t))

(defn on
  [q & clauses]
  (defer
    (->> (map condition* clauses)
         (into-array org.jooq.Condition)
         (.on @q))))

(defn where
  "Create where clause with variable number
  of conditions (that are implicitly combined
  with `and` logical operator)."
  [q & clauses]
  (defer
    (->> (map condition* clauses)
         (into-array org.jooq.Condition)
         (.where @q))))

(defn exists
  "Create an exists condition."
  [select']
  (defer
    (DSL/exists select')))

(defn group-by
  [q & fields]
  (defer
    (->> (map (comp field* unwrap*) fields)
         (into-array org.jooq.GroupField)
         (.groupBy @q))))

(defn having
  "Create having clause with variable number
  of conditions (that are implicitly combined
  with `and` logical operator)."
  [q & clauses]
  (defer
    (->> (map condition* clauses)
         (into-array org.jooq.Condition)
         (.having @q))))

(defn order-by
  [q & clauses]
  (defer
    (->> (map sort-field* clauses)
         (into-array org.jooq.SortField)
         (.orderBy @q))))

(defn for-update
  [q & fields]
  (defer
    (let [q (.forUpdate @q)]
      (if (seq fields)
        (->> (map field* fields)
             (into-array org.jooq.Field)
             (.of q))
        q))))

(defn limit
  "Creates limit clause."
  [q num]
  (defer
    (.limit @q num)))

(defn offset
  "Creates offset clause."
  [q num]
  (defer
    (.offset @q num)))

(defn union
  [& clauses]
  (defer
    (reduce (fn [acc v] (.union acc @v))
            (-> clauses first deref)
            (-> clauses rest))))

(defn union-all
  [& clauses]
  (defer
    (reduce (fn [acc v] (.unionAll acc @v))
            (-> clauses first deref)
            (-> clauses rest))))

(defn returning
  [t & fields]
  (defer
    (if (= (count fields) 0)
      (.returning @t)
      (.returning @t (->> (map field* fields)
                          (into-array org.jooq.Field))))))

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

(defn name
  [v]
  (defer
    (name* v)))

(defn with
  "Create a WITH clause"
  [& tables]
  (defer
    (->> (map table* tables)
         (into-array org.jooq.CommonTableExpression)
         (DSL/with))))

(defn with-fields
  "Add a list of fields to this name to make this name a DerivedColumnList."
  [n & fields]
  (defer
    (let [fields (->> (map clojure.core/name fields)
                      (into-array String))]
      (.fields @n fields))))

(defmacro row
  [& values]
  `(DSL/row ~@(map (fn [x#] `(unwrap* ~x#)) values)))

(defn values
  [& rows]
  (defer
    (->> (into-array org.jooq.RowN rows)
         (DSL/values))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Insert statement
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defmacro insert-into
  [t & fields]
  `(DSL/insertInto (table* ~t)
                   ~@(map (fn [x#] `(field* ~x#)) fields)))

(defmacro insert-values
  [t & values]
  `(.values ~t ~@values))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Update statement
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn update
  [t]
  (defer
    (DSL/update (table* t))))

(defn set
  [t f v]
  (defer
    (let [v (unwrap* v)
          t (unwrap* t)]
      (if (instance? org.jooq.Row f)
        (.set t f v)
        (.set t (field* f) v)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Delete statement
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn delete
  [t]
  (defer
    (DSL/delete (table* t))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DDL
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^{:doc "Datatypes translation map" :dynamic true}
  *datatypes*
  {:pg/varchar PostgresDataType/VARCHAR})

(defn truncate
  [t]
  (defer
    (-> (table* t)
        (DSL/truncate))))

(defn alter-table
  [t]
  (defer
    (-> (table* t)
        (DSL/alterTable))))

(defn- datatype-transformer
  [opts ^org.jooq.DataType acc attr]
  (case attr
    :length (.length acc (attr opts))
    :null   (.nullable acc (attr opts))))

(defn set-column-type
  [t name datatype & [opts]]
  (defer
    (let [^org.jooq.AlterTableFinalStep t (.alter @t (field* name))]
      (->> (reduce (partial datatype-transformer opts)
                   (datatype *datatypes*)
                   (keys opts))
           (.set t)))))

(defn add-column
  "Add column to alter table step."
  [t name datatype & [opts]]
  (defer
    (->> (reduce (partial datatype-transformer opts)
                 (datatype *datatypes*)
                 (keys opts))
         (.add @t (field* name)))))

(defn drop-column
  "Drop column from alter table step."
  [t name & [type]]
  (defer
    (let [^org.jooq.AlterTableDropStep t (.drop @t (field* name))]
      (case type
        :cascade (.cascade t)
        :restrict (.restrict t)
        t))))

(defn drop-table
  "Drop table statement constructor."
  [t]
  (defer
    (-> (table* t)
        (DSL/dropTable))))
