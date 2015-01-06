;; Copyright (c) 2014-2015, Andrey Antukh
;; All rights reserved.
;;
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions are met:
;;
;; * Redistributions of source code must retain the above copyright notice, this
;;   list of conditions and the following disclaimer.
;;
;; * Redistributions in binary form must reproduce the above copyright notice,
;;   this list of conditions and the following disclaimer in the documentation
;;   and/or other materials provided with the distribution.
;;
;; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;; AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
;; IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
;; DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
;; FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
;; DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
;; SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
;; CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
;; OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
;; OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

(ns suricatta.dsl
  "Sql building dsl"
  (:refer-clojure :exclude [val group-by and or not name set update])
  (:require [suricatta.core :as core]
            [suricatta.impl :as impl]
            [suricatta.types :as types :refer [defer]]
            [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration
           org.jooq.util.postgres.PostgresDataType
           org.jooq.util.mariadb.MariaDBDataType
           org.jooq.util.mysql.MySQLDataType
           suricatta.types.Context))

(def ^{:doc "Datatypes translation map" :dynamic true}
  *datatypes*
  {:pg/varchar PostgresDataType/VARCHAR
   :pg/any PostgresDataType/ANY
   :pg/bigint PostgresDataType/BIGINT
   :pg/bigserial PostgresDataType/BIGSERIAL
   :pg/boolean PostgresDataType/BOOLEAN
   :pg/date PostgresDataType/DATE
   :pg/decimal PostgresDataType/DECIMAL
   :pg/real PostgresDataType/REAL
   :pg/double PostgresDataType/DOUBLEPRECISION
   :pg/int4 PostgresDataType/INT4
   :pg/int2 PostgresDataType/INT2
   :pg/int8 PostgresDataType/INT8
   :pg/integer PostgresDataType/INTEGER
   :pg/serial PostgresDataType/SERIAL
   :pg/serial4 PostgresDataType/SERIAL4
   :pg/serial8 PostgresDataType/SERIAL8
   :pg/smallint PostgresDataType/SMALLINT
   :pg/text PostgresDataType/TEXT
   :pg/time PostgresDataType/TIME
   :pg/timetz PostgresDataType/TIMETZ
   :pg/timestamp PostgresDataType/TIMESTAMP
   :pg/timestamptz PostgresDataType/TIMESTAMPTZ
   :pg/uuid PostgresDataType/UUID
   :pg/char PostgresDataType/CHAR
   :pg/bytea PostgresDataType/BYTEA
   :pg/numeric PostgresDataType/NUMERIC
   :maria/bigint MariaDBDataType/BIGINT
   :maria/ubigint MariaDBDataType/BIGINTUNSIGNED
   :maria/binary MariaDBDataType/BINARY
   :maria/blob MariaDBDataType/BLOB
   :maria/bool MariaDBDataType/BOOL
   :maria/boolean MariaDBDataType/BOOLEAN
   :maria/char MariaDBDataType/CHAR
   :maria/date MariaDBDataType/DATE
   :maria/datetime MariaDBDataType/DATETIME
   :maria/decimal MariaDBDataType/DECIMAL
   :maria/double MariaDBDataType/DOUBLE
   :maria/enum MariaDBDataType/ENUM
   :maria/float MariaDBDataType/FLOAT
   :maria/int MariaDBDataType/INT
   :maria/integer MariaDBDataType/INTEGER
   :maria/uint MariaDBDataType/INTEGERUNSIGNED
   :maria/longtext MariaDBDataType/LONGTEXT
   :maria/mediumint MariaDBDataType/MEDIUMINT
   :maria/real MariaDBDataType/REAL
   :maria/smallint MariaDBDataType/SMALLINT
   :maria/time MariaDBDataType/TIME
   :maria/timestamp MariaDBDataType/TIMESTAMP
   :maria/varchar MariaDBDataType/VARCHAR
   :mysql/bigint MySQLDataType/BIGINT
   :mysql/ubigint MySQLDataType/BIGINTUNSIGNED
   :mysql/binary MySQLDataType/BINARY
   :mysql/blob MySQLDataType/BLOB
   :mysql/bool MySQLDataType/BOOL
   :mysql/boolean MySQLDataType/BOOLEAN
   :mysql/char MySQLDataType/CHAR
   :mysql/date MySQLDataType/DATE
   :mysql/datetime MySQLDataType/DATETIME
   :mysql/decimal MySQLDataType/DECIMAL
   :mysql/double MySQLDataType/DOUBLE
   :mysql/enum MySQLDataType/ENUM
   :mysql/float MySQLDataType/FLOAT
   :mysql/int MySQLDataType/INT
   :mysql/integer MySQLDataType/INTEGER
   :mysql/uint MySQLDataType/INTEGERUNSIGNED
   :mysql/longtext MySQLDataType/LONGTEXT
   :mysql/mediumint MySQLDataType/MEDIUMINT
   :mysql/real MySQLDataType/REAL
   :mysql/smallint MySQLDataType/SMALLINT
   :mysql/time MySQLDataType/TIME
   :mysql/timestamp MySQLDataType/TIMESTAMP
   :mysql/varchar MySQLDataType/VARCHAR})

(defn- make-datatype
  [{:keys [type] :as opts}]
  (reduce (fn [dt [attname attvalue]]
            (case attname
              :length (.length dt attvalue)
              :null (.nullable dt attvalue)
              :precision (.precision dt attvalue)
              dt))
          (get *datatypes* type)
          (into [] opts)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocols for constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol ISortField
  (sort-field* [_] "Sort field constructor"))

(defprotocol ITable
  (table* [_] "Table constructor."))

(defprotocol IName
  (name* [_] "Name constructor (mainly used with CTE)"))

(defprotocol ITableCoerce
  (as-table* [_ params] "Table alias constructor"))

(defprotocol IFieldCoerce
  (as-field* [_ params] "Field alias constructor"))

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

(defmulti field* (comp class first vector))

(defmethod field* java.lang.String
  [^String name & [type]]
  (let [type (get *datatypes* type)]
    (if type
      (DSL/field name type)
      (DSL/field name))))

(defmethod field* clojure.lang.Keyword
  [name & args]
  (apply field* (clojure.core/name name) args))

(defmethod field* org.jooq.Field
  [field & args]
  field)

(defmethod field* org.jooq.impl.Val
  [field & args]
  field)

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
    (let [^org.jooq.Field field (field* (first v))
          ^org.jooq.SortField field (case (second v)
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

  org.jooq.Condition
  (condition* [c] c)

  clojure.lang.PersistentVector
  (condition* [v]
    (let [sql    (first v)
          params (rest v)]
      (->> (map unwrap* params)
           (into-array Object)
           (DSL/condition sql)))))

(extend-protocol IVal
  Object
  (val* [v] (DSL/val v)))

(extend-protocol IFieldCoerce
  org.jooq.FieldLike
  (as-field* [n args]
    (let [^String alias (first args)]
      (.asField n alias)))

  suricatta.types.Deferred
  (as-field* [n args] (as-field* @n args)))

(extend-protocol ITableCoerce
  org.jooq.Name
  (as-table* [^org.jooq.Name n args]
    (.as n ^String (first args)))

  org.jooq.DerivedColumnList
  (as-table* [n args]
    (.as n (first args)))

  org.jooq.TableLike
  (as-table* [n args]
    (let [^String alias (first args)]
      (->> (into-array String (rest args))
           (.asTable n alias))))

  suricatta.types.Deferred
  (as-table* [t args]
    (as-table* @t args)))

(extend-protocol IDeferred
  Object
  (unwrap* [self]
    (impl/wrap-if-need self))

  suricatta.types.Deferred
  (unwrap* [self]
    (unwrap* @self)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Common DSL functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn as-table
  "Coerce querypart to table expression."
  [o & args]
  (defer
    (->> (map unwrap* args)
         (as-table* o))))

(defn as-field
  "Coerce querypart to field expression."
  [o & args]
  (defer
    (->> (map unwrap* args)
         (as-field* o))))

(defn field
  "Create a field instance."
  [data & [{:keys [alias] :as opts}]]
  (defer
    (let [f (field* data)]
      (if alias
        (.as f (clojure.core/name alias))
        f))))

(defn val
  [v]
  (val* v))

(defn table
  "Create a table instance."
  [data & [{:keys [alias] :as opts}]]
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
  [t]
  (-> (table* t)
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

(defn cross-join
  [step tlike]
  (defer
    (let [t (table* (unwrap* tlike))
          step (unwrap* step)]
      (.crossJoin step t))))

(defn full-outer-join
  [step tlike]
  (defer
    (let [t (table* (unwrap* tlike))
          step (unwrap* step)]
      (.fullOuterJoin step t))))

(defn left-outer-join
  [step tlike]
  (defer
    (let [t (table* (unwrap* tlike))
          step (unwrap* step)]
      (.leftOuterJoin step t))))

(defn right-outer-join
  [step tlike]
  (defer
    (let [t (table* (unwrap* tlike))
          step (unwrap* step)]
      (.rightOuterJoin step t))))

(defmulti on (comp class unwrap* first vector))

(defmethod on org.jooq.SelectOnStep
  [step & clauses]
  (defer
    (->> (map condition* clauses)
         (into-array org.jooq.Condition)
         (.on (unwrap* step)))))

(defmethod on org.jooq.TableOnStep
  [step & clauses]
  (defer
    (->> (map condition* clauses)
         (into-array org.jooq.Condition)
         (.on (unwrap* step)))))

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

(defn insert-into
  [t]
  (defer
    (DSL/insertInto (table* t))))

(defn insert-values
  [t values]
  (defer
    (-> (fn [acc [k v]]
          (.set acc (field* k) (unwrap* v)))
        (reduce (unwrap* t) values)
        (.newRecord))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Update statement
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn update
  "Returns empty UPDATE statement."
  [t]
  (defer
    (DSL/update (table* t))))

(defn set
  "Attach values to the UPDATE statement."
  ([t kv]
   (defer
     (let [t (unwrap* t)]
       (reduce (fn [acc [k v]]
                 (.set t (field* k) v))
               t kv))))
  ([t k v]
   (defer
     (let [v (unwrap* v)
           t (unwrap* t)]
       (if (instance? org.jooq.Row k)
         (.set t k v)
         (.set t (field* k) v))))))

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

(defn truncate
  [t]
  (defer
    (DSL/truncate (table* t))))

(defn alter-table
  "Creates new and empty alter table expression."
  [name]
  (defer
    (-> (unwrap* name)
        (table*)
        (DSL/alterTable))))

(defn create-table
  [name]
  (defer
    (-> (unwrap* name)
        (table*)
        (DSL/createTable))))

(defn drop-table
  "Drop table statement constructor."
  [t]
  (defer
    (DSL/dropTable (table* t))))

;; Columns functions
(defmulti add-column (comp class unwrap* first vector))

(defmethod add-column org.jooq.AlterTableStep
  [step name & [{:keys [default] :as opts}]]
  (defer
    (let [step (unwrap* step)
          name (field* name)
          type (make-datatype opts)
          step (.add step name type)]
        (if default
           (.setDefault step (field* default))
           step))))

(defmethod add-column org.jooq.CreateTableAsStep
  [step name & [{:keys [default] :as opts}]]
  (defer
    (let [step (unwrap* step)
          name (field* name)
          type (make-datatype opts)
          type (.defaulted type true)]
      (.column step name type))))

(defn alter-column
  [step name & [{:keys [type default null length] :as opts}]]
  (defer
    (let [step (-> (unwrap* step)
                   (.alter (field* name)))]
      (when (clojure.core/and (clojure.core/or null length) (clojure.core/not type))
        (throw (IllegalArgumentException.
                "For change null or length you should specify type.")))
      (when type
        (.set step (make-datatype opts)))
      (when default
        (.defautValue step default))
      step)))

(defn drop-column
  "Drop column from alter table step."
  [step name & [type]]
  (defer
    (let [step (-> (unwrap* step)
                   (.drop (field* name)))]
      (case type
        :cascade (.cascade step)
        :restrict (.restrict step)
        step))))

;; Index functions

(defmethod on org.jooq.CreateIndexStep
  [step table field & extrafields]
  (defer
    (let [fields (->> (concat [field] extrafields)
                      (map (comp field* unwrap*))
                      (into-array org.jooq.Field))]
      (.on (unwrap* step) (table* table) fields))))

(defn create-index
  [indexname]
  (defer
    (let [indexname (clojure.core/name indexname)]
      (DSL/createIndex indexname))))

(defn drop-index
  [indexname]
  (defer
    (let [indexname (clojure.core/name indexname)]
      (DSL/dropIndex indexname))))


;; Sequence functions

(defn create-sequence
  [seqname]
  (defer
    (let [seqname (clojure.core/name seqname)]
      (DSL/createSequence seqname))))

(defn alter-sequence
  [seqname restart]
  (defer
    (let [seqname (clojure.core/name seqname)
          step    (DSL/alterSequence seqname)]
      (if (true? restart)
        (.restart step)
        (.restartWith step restart)))))

(defn drop-sequence
  ([seqname] (drop-sequence seqname false))
  ([seqname ifexists]
   (defer
     (let [seqname (clojure.core/name seqname)]
       (if ifexists
         (DSL/dropSequenceIfExists seqname)
         (DSL/dropSequence seqname))))))
