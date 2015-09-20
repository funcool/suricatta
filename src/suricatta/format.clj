;; Copyright (c) 2014, Andrey Antukh <niwi@niwi.nz>
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

(ns suricatta.format
  "Sql formatting related code."
  (:require [suricatta.core :as core]
            [suricatta.proto :as proto]
            [suricatta.types :as types]
            [suricatta.impl :as impl])
  (:import org.jooq.Configuration
           org.jooq.DSLContext
           org.jooq.impl.DefaultConfiguration
           org.jooq.impl.DSL))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IRenderer
  org.jooq.Query
  (-get-sql [q type dialect]
    (let [^Configuration conf (DefaultConfiguration.)
          ^DSLContext context (DSL/using conf)]
      (when dialect
        (.set conf (impl/translate-dialect dialect)))
      (condp = type
        nil      (.render context q)
        :named   (.renderNamedParams context q)
        :indexed (.render context q)
        :inlined (.renderInlined context q))))

  (-get-bind-values [q]
    (let [^Configuration conf (DefaultConfiguration.)
          ^DSLContext context (DSL/using conf)]
      (.extractBindValues context q)))

  suricatta.types.Deferred
  (-get-sql [self type dialect]
    (proto/-get-sql @self type dialect))

  (-get-bind-values [self]
    (proto/-get-bind-values @self)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-sql
  "Renders a query sql into string."
  ([q]
   (proto/-get-sql q nil nil))
  ([q {:keys [type dialect] :as opts}]
   (proto/-get-sql q type dialect)))

(defn get-bind-values
  "Get bind values from query"
  [q]
  (proto/-get-bind-values q))

(defn sqlvec
  "Get sql with bind values in a `sqlvec` format."
  [q]
  (apply vector
         (get-sql q {:type :indexed})
         (get-bind-values q)))
