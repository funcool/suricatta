;; Copyright (c) 2014-2015, Andrey Antukh <niwi@niwi.nz>
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
            [suricatta.impl :as impl]
            jdbc.core
            jdbc.proto)
  (:import org.jooq.DSLContext
           org.jooq.SQLDialect
           org.jooq.TransactionContext
           org.jooq.TransactionProvider
           org.jooq.exception.DataAccessException;
           org.jooq.impl.DefaultTransactionContext
           org.jooq.Configuration
           org.jooq.impl.DefaultConfiguration
           org.jooq.tools.jdbc.JDBCUtils
           java.sql.Connection))

(defn context
  "Context constructor."
  ([dbspec] (context dbspec {}))
  ([dbspec opts]
   (let [^Connection connection (-> (jdbc.core/connection dbspec opts)
                                    (jdbc.proto/connection))
         ^SQLDialect dialect (if (:dialect dbspec)
                               (impl/translate-dialect (:dialect dbspec))
                               (JDBCUtils/dialect connection))
         ^Configuration conf (doto (DefaultConfiguration.)
                               (.set dialect)
                               (.set connection))]
      (types/->context conf))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; SQL Executor
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn execute
  "Execute a query and return a number of rows affected."
  ([q] (proto/-execute q nil))
  ([ctx q] (proto/-execute q ctx)))

(defn fetch
  "Fetch eagerly results executing a query.

  This function returns a vector of records (default) or
  rows (depending on specified opts). Resources are relased
  inmediatelly without specific explicit action for it."
  ([q] (proto/-fetch q nil {}))
  ([ctx q] (proto/-fetch q ctx {}))
  ([ctx q opts] (proto/-fetch q ctx opts)))

(def fetch-one (comp first fetch))

(defn query
  "Mark a query for reuse the prepared statement.

  This function should be used with precaution and
  close method should be called when query is not
  longer needed. In almost all cases you should not
  need use this function."
  [ctx querylike]
  (proto/-query querylike ctx))

(defn fetch-lazy
  "Fetch lazily results executing a query.

  This function returns a cursor instead of result.
  You should explicitly close the cursor at the end of
  iteration for release resources."
  ([ctx q] (proto/-fetch-lazy q ctx {}))
  ([ctx q opts] (proto/-fetch-lazy q ctx {})))

(defn cursor->lazyseq
  "Transform a cursor in a lazyseq.

  The returned lazyseq will return values until a cursor
  is closed or all values are fetched."
  ([cursor] (impl/cursor->lazyseq cursor {}))
  ([cursor opts] (impl/cursor->lazyseq cursor opts)))

(defn load-into
  "Load data into a table. Supports csv and json formats."
  ([ctx tablename data] (load-into ctx tablename data {}))
  ([ctx tablename data opts]
   (impl/load-into ctx tablename data opts)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- transaction-context
  [^Configuration conf]
  (let [transaction (atom nil)
        cause       (atom nil)]
    (reify org.jooq.TransactionContext
      (configuration [_] conf)
      (settings [_] (.settings conf))
      (dialect [_] (.dialect conf))
      (family [_] (.family (.dialect conf)))
      (transaction [_] @transaction)
      (transaction [self t] (reset! transaction t) self)
      (cause [_] @cause)
      (cause [self c] (reset! cause c) self))))

(defn atomic-apply
  "Execute a function in one transaction
  or subtransaction."
  [ctx func & args]
  (let [^Configuration conf (.derive (proto/-get-config ctx))
        ^TransactionContext txctx (transaction-context conf)
        ^TransactionProvider provider (.transactionProvider conf)]
    (doto conf
      (.data "suricatta.rollback" false)
      (.data "suricatta.transaction" true))
    (try
      (.begin provider txctx)
      (let [result (apply func (types/->context conf) args)
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

(defmacro atomic
  "Convenience macro for execute a computation
  in a transaction or subtransaction."
  [ctx & body]
  `(atomic-apply ~ctx (fn [~ctx] ~@body)))

(defn set-rollback!
  "Mark current transaction for rollback.

  This function is not safe and it not aborts
  the execution of current function, it only
  marks the current transaction for rollback."
  [ctx]
  (let [^Configuration conf (proto/-get-config ctx)]
    (.data conf "suricatta.rollback" true)
    ctx))
