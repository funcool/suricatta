;; Copyright (c) 2014-2016 Andrey Antukh <niwi@niwi.nz>
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

(ns suricatta.transaction
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
           org.jooq.impl.DefaultConfiguration
           org.jooq.tools.jdbc.JDBCUtils
           java.sql.Connection))

(defn transaction-context
  {:internal true}
  [^Configuration conf]
  (let [transaction (atom nil)
        cause       (atom nil)]
    (reify TransactionContext
      (configuration [_] conf)
      (settings [_] (.settings conf))
      (dialect [_] (.dialect conf))
      (family [_] (.family (.dialect conf)))
      (transaction [_] @transaction)
      (transaction [self t] (reset! transaction t) self)
      (cause [_] @cause)
      (cause [self c] (reset! cause c) self))))

(defn apply-atomic
  [ctx func & args]
  (let [^Configuration conf (.derive (proto/-config ctx))
        ^TransactionContext txctx (transaction-context conf)
        ^TransactionProvider provider (.transactionProvider conf)]
    (doto conf
      (.data "suricatta.rollback" false)
      (.data "suricatta.transaction" true))
    (try
      (.begin provider txctx)
      (let [result (apply func (types/context conf) args)
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

(defn set-rollback!
  [ctx]
  (let [^Configuration conf (proto/-config ctx)]
    (.data conf "suricatta.rollback" true)
    ctx))
