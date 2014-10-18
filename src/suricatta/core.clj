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
  ([query] (proto/execute query nil))
  ([ctx query] (proto/execute query ctx)))

(defn fetch
  "Fetch eagerly results executing a query."
  ([q] (proto/fetch q nil {}))
  ([ctx q] (proto/fetch q ctx {}))
  ([ctx q opts] (proto/fetch q ctx opts)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn atomic
  [^Context ctx func]
  (let [^DSLContext context (proto/get-context ctx)]
    (.transactionResult context (reify TransactionalCallable
                                  (run [_ ^Configuration conf]
                                    (let [ctx (types/->context (.-conn ctx) conf false)]
                                      (apply func [ctx])))))))

(defmacro with-atomic
  [ctx & body]
  `(atomic ~ctx (fn [~ctx] ~@body)))
