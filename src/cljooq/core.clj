(ns cljooq.core
  "High level sql toolkit for Clojure"
  (:require [cljooq.types :as types]
            [cljooq.proto :as proto]
            [cljooq.impl :as impl])
  (:import org.jooq.impl.DSL
           org.jooq.SQLDialect))

(defn context
  "Context constructor."
  [opts]
  (proto/make-context opts))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Executor
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn execute
  "Execute a query and return a number of rows affected."
  ([query]
     (assert (or (types/query? query) (types/result-query? query))
             "execute/1 only works with query/queryresult instances")
     (proto/execute query nil))
  ([ctx query] (proto/execute query ctx)))

(defn fetch
  "Fetch eagerly results executing a query."
  ([q] (fetch q {}))

  ([ctx q]
     (cond
      (types/context? ctx)
      (proto/fetch q ctx {})

      (types/result-query? ctx)
      (proto/fetch ctx nil q)))

  ([ctx q opts]
     (proto/fetch q ctx opts)))

(defn query
  "Creates a Query instance."
  [ctx q]
  (proto/query q ctx))

(defn result-query
  "ResultQuery constructor"
  ([ctx q]
    (proto/result-query q ctx {}))

  ([ctx q opts]
    (proto/result-query q ctx opts)))
