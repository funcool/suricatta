(ns cljooq.core
  "High level sql toolkit for Clojure"
  (:require [cljooq.types :as types]
            [cljooq.proto :as proto]))

(defn context
  "Creates new context. It usually created from
  dbspec or open jdbc connection."
  [source]
  (proto/make-context source))

(defn execute
  "Execute a query and return a number of rows affected."
  ([query]
     (assert (or (types/query? query) (types/result-query? query))
             "execute/1 only works with query/queryresult instances")
     (proto/execute query nil))
  ([ctx query] (proto/execute query (proto/get-context ctx))))

(defn fetch
  "Fetch eagerly results executing a query."
  ([query'] (fetch query' {}))

  ([ctx query']
     (cond
      (types/context? ctx)
      (proto/fetch query' (proto/get-context ctx) {})

      (types/result-query? ctx)
      (proto/fetch ctx nil query')))

  ([ctx query' opts]
     (proto/fetch query' ctx opts)))

(defn query
  "Creates a Query instance."
  [ctx query']
  (proto/query query' (proto/get-context ctx)))

(defn result-query
  "ResultQuery constructor"
  ([ctx query']
    (proto/result-query query' (proto/get-context ctx) {}))

  ([ctx query' opts]
    (proto/result-query query' (proto/get-context ctx) opts)))

(defn get-sql
  "Get SQL string from previously builded query."
  ([query] (get-sql query :indexed))
  ([query type]
     (proto/get-sql query type)))

(defn get-bind-values
  "Get bind values from previously builded query."
  [query]
  (proto/get-bind-values query))

(defn sqlvec
  "Get sql with bind values in a `sqlvec` format."
  [query]
  (proto/sqlvec query))
