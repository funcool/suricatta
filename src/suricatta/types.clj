(ns suricatta.types
  (:require [suricatta.proto :as proto])
  (:import java.sql.Connection
           org.jooq.impl.DSL
           org.jooq.Configuration
           org.jooq.SQLDialect))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Context
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Context [^Connection conn
                  ^Configuration conf
                  ^Boolean closeable]
  proto/IContext
  (get-context [_] (DSL/using conf))
  (get-configuration [_] conf)

  java.io.Closeable
  (close [_]
    (when closeable
      (.set conf (org.jooq.impl.NoConnectionProvider.))
      (.close conn))))

(defn ->context
  "Context instance constructor."
  ([^Connection conn ^Configuration conf]
     (Context. conn conf true))
  ([^Connection conn ^Configuration conf ^Boolean closeable]
     (Context. conn conf closeable)))

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

