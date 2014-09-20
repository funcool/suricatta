(ns cljooq.dsl
  "Sql building dsl"
  (:import org.jooq.impl.DSL))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord QueryPart [q])
(defn query-part [q] (QueryPart. q))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Protocols for constructors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol IField
  (field [_] "Field constructor"))

(defprotocol ITable
  (table [_] "Table constructor"))

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
  (table [kw] (table (name kw))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; DSL functions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn select
  [& args]
  (->> (mapv field args)
       (into-array org.jooq.Field)
       (DSL/select)))

(defn from
  [q & tables]
  (->> (map table tables)
       (into-array org.jooq.Table)
       (.from q)))
