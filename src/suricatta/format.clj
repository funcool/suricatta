(ns suricatta.format
  "Sql formatting related code."
  (:require [suricatta.core :as core]
            [suricatta.proto :as proto]
            [suricatta.types :as types]
            [suricatta.impl :as impl])
  (:import org.jooq.impl.DefaultConfiguration
           org.jooq.impl.DSL))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IRenderer
  org.jooq.Query
  (get-sql [q type dialect]
    (let [conf (DefaultConfiguration.)
          ctx  (DSL/using conf)]
      (when dialect
        (.set conf (impl/translate-dialect dialect)))
      (condp = type
        nil      (.render ctx q)
        :named   (.renderNamedParams ctx q)
        :indexed (.render ctx q)
        :inlined (.renderInlined ctx q))))

  (get-bind-values [q]
    (let [conf (DefaultConfiguration.)
          ctx  (DSL/using conf)]
      (.extractBindValues ctx q)))

  suricatta.types.Deferred
  (get-sql [self type dialect]
    (proto/get-sql @self type dialect))

  (get-bind-values [self]
    (proto/get-bind-values @self)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Public Api
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-sql
  "Renders a query sql into string."
  ([q]
     (proto/get-sql q nil nil))
  ([q {:keys [type dialect] :as opts}]
     (proto/get-sql q type dialect)))

(defn get-bind-values
  "Get bind values from query"
  [q]
  (proto/get-bind-values q))

(defn sqlvec
  "Get sql with bind values in a `sqlvec` format."
  [q]
  (apply vector
         (get-sql q {:type :indexed})
         (get-bind-values q)))
