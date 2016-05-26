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

(ns suricatta.types
  (:require [suricatta.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.ResultQuery
           org.jooq.Configuration
           org.jooq.ConnectionProvider
           org.jooq.SQLDialect
           java.sql.Connection))

(deftype Context [^Configuration conf]
  proto/IContextHolder
  (-context [_] (DSL/using conf))
  (-config [_] conf)

  java.io.Closeable
  (close [_]
    (let [^ConnectionProvider provider (.connectionProvider conf)
          ^Connection connection (.acquire provider)]
      (.close connection)
      (.set conf (org.jooq.impl.NoConnectionProvider.)))))

(defn context
  "Context instance constructor."
  [^Configuration conf]
  (Context. conf))

(defn context?
  [ctx]
  (instance? Context ctx))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Deferred Computation (without caching the result unlike delay)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Deferred [^clojure.lang.IFn func]
  clojure.lang.IDeref
  (deref [_] (func)))

(defn ->deferred
  [o]
  (Deferred. o))

(defmacro defer
  [& body]
  `(let [func# (fn [] ~@body)]
     (->deferred func#)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Query [^ResultQuery query ^Configuration conf]
  java.io.Closeable
  (close [_]
    (.close query))

  proto/IContextHolder
  (-context [_] (DSL/using conf))
  (-config [_] conf))

(defn query
  [query conf]
  (Query. query conf))
