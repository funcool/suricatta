;; Copyright (c) 2014, Andrey Antukh
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
      (let [result (func (types/->context conf))
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
