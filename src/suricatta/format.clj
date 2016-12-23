;; Copyright (c) 2014-2017 Andrey Antukh <niwi@niwi.nz>
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

(defn sql
  "Renders a query sql into string."
  ([q]
   (proto/-sql q nil nil))
  ([q {:keys [type dialect] :or {type :indexed} :as opts}]
   (proto/-sql q type dialect)))

(def ^:deprecated get-sql
  "Deprecated alias to get-sql."
  sql)

(defn bind-values
  "Get bind values from query"
  [q]
  (proto/-bind-values q))

(def ^:deprecated get-bind-values
  "Deprecated alias to bind-values."
  bind-values)

(defn sqlvec
  "Get sql with bind values in a `sqlvec` format."
  ([q] (sqlvec q nil))
  ([q opts]
   (apply vector
          (sql q opts)
          (bind-values q))))
