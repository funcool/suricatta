(ns cljooq.dsl
  "Sql building dsl"
  (:import org.jooq.impl.DSL))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocols for constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IField
  (field [_] "Field constructor"))

(defprotocol ITable
  (table [_] "Table constructor"))

(defprotocol ICondition
  (condition [_] "Condition constructor"))

(defprotocol IVal
  (val [_] "Val constructor"))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocol Implementations
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol IField
  java.lang.String
  (field [s] (DSL/field s))

  clojure.lang.Keyword
  (field [kw] (field (name kw))))

(extend-protocol ITable
  java.lang.String
  (table [s] (DSL/table s))

  clojure.lang.Keyword
  (table [kw] (table (name kw)))

  org.jooq.Table
  (table [t] t))

(extend-protocol ICondition
  java.lang.String
  (condition [s] (DSL/condition s))

  org.jooq.impl.CombinedCondition
  (condition [c] c)

  org.jooq.impl.SQLCondition
  (condition [c] c)

  clojure.lang.PersistentVector
  (condition [v]
    (let [sql    (first v)
          params (rest v)]
      (->> (into-array Object params)
           (DSL/condition sql)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DSL functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn select
  "Start select statement."
  [& fields]
  (->> (map field fields)
       (into-array org.jooq.Field)
       (DSL/select)))

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

(defn on
  [q cond]
  (.on q cond))

(defn where
  "Create where clause with variable number
  of conditions (that are implicitly combined
  with `and` logical operator)."
  [q & clauses]
  (->> (map condition clauses)
       (into-array org.jooq.Condition)
       (.where q)))

(defn exists
  "Create an exists condition."
  [q select']
  (DSL/exists select'))

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
  (let [conditions (map condition conditions)]
    (reduce (fn [acc v] (.and acc v))
            (first conditions)
            (rest conditions))))

(defn or
  "Logican operator `or`."
  [& conditions]
  (let [conditions (map condition conditions)]
    (reduce (fn [acc v] (.or acc v))
            (first conditions)
            (rest conditions))))

(defn not
  "Negate a condition."
  [c]
  (DSL/not c))

