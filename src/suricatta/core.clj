(ns suricatta.core
  "High level sql toolkit for Clojure"
  (:require [suricatta.types :as types]
            [suricatta.proto :as proto]
            [suricatta.impl :as impl])
  (:import org.jooq.DSLContext
           org.jooq.SQLDialect
           org.jooq.TransactionalCallable
           org.jooq.Configuration
           suricatta.types.Context))

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
  ([^Context ctx query] (proto/execute query ctx)))

(defn fetch
  "Fetch eagerly results executing a query."
  ([q] (proto/fetch q nil {}))
  ([ctx q]
     (cond
      (types/context? ctx)
      (proto/fetch q ctx {})

      (types/result-query? ctx)
      (proto/fetch ctx nil q)))
  ([^Context ctx q opts]
     (proto/fetch q ctx opts)))

(defn query
  "Creates a Query instance."
  [^Context ctx q]
  (proto/query q ctx))

(defn result-query
  "ResultQuery constructor"
  ([^Context ctx q]
    (proto/result-query q ctx {}))

  ([^Context ctx q opts]
    (proto/result-query q ctx opts)))

(defn transaction
  [^Context ctx func]
  (let [^DSLContext context (proto/get-context ctx)]
    (.transactionResult context (reify TransactionalCallable
                                  (run [_ ^Configuration conf]
                                    (let [ctx (types/context (.-conn ctx) conf false)]
                                      (apply func [ctx])))))))

(defmacro with-transaction
  [ctx & body]
  `(transaction ~ctx (fn [~ctx] ~@body)))
