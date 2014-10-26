(ns suricatta.core
  "High level sql toolkit for Clojure"
  (:require [suricatta.types :as types]
            [suricatta.proto :as proto]
            [suricatta.impl :as impl])
  (:import org.jooq.DSLContext
           org.jooq.SQLDialect
           org.jooq.TransactionContext
           org.jooq.TransactionProvider
           org.jooq.exception.DataAccessException;
           org.jooq.impl.DefaultTransactionContext
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
  ([q] (proto/execute q nil))
  ([ctx q] (proto/execute q ctx)))

(defn fetch
  "Fetch eagerly results executing a query."
  ([q] (proto/fetch q nil {}))
  ([ctx q] (proto/fetch q ctx {}))
  ([ctx q opts] (proto/fetch q ctx opts)))

(defn query
  [ctx querylike]
  (proto/query querylike ctx))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- transaction-context
  [^Configuration conf]
  (let [transaction (atom nil)
        cause       (atom nil)]
    (reify org.jooq.TransactionContext
      (configuration [_] conf)
      ;; jOOQ 3.5.x
      ;; (settings [_] (.settings conf))
      ;; (dialect [_] (.dialect conf))
      ;; (family [_] (.family (.dialect conf)))
      (transaction [_] @transaction)
      (transaction [self t] (reset! transaction t) self)
      (cause [_] @cause)
      (cause [self c] (reset! cause c) self))))

(defn atomic
  "Execute a function in one transaction
  or subtransaction."
  [^Context ctx func]
  (let [^Configuration conf (.derive (proto/get-configuration ctx))
        ^TransactionContext txctx (transaction-context conf)
        ^TransactionProvider provider (.transactionProvider conf)]
    (.data conf "suricatta.rollback" false)
    (try
      (.begin provider txctx)
      (let [result    (-> (types/->context (.-conn ctx) conf)
                          (func))
            rollback? (.data conf "suricatta.rollback")]
        (if rollback?
          (.rollback provider txctx)
          (.commit provider txctx))
        result)
      (catch Exception cause
        (.rollback provider (.cause txctx cause))
        (if (instance? RuntimeException cause)
          (throw cause)
          (throw (DataAccessException. "Rollback caused" cause)))))))

(defmacro with-atomic
  "Convenience macro for execute a computation
  in a transaction or subtransaction."
  [ctx & body]
  `(atomic ~ctx (fn [~ctx] ~@body)))

(defn set-rollback!
  "Mark current transaction for rollback.

  This function is not safe and it not aborts
  the execution of current function, it only
  marks the current transaction for rollback."
  [^Context ctx]
  (let [^Configuration conf (proto/get-configuration ctx)]
    (.data conf "suricatta.rollback" true)
    ctx))
