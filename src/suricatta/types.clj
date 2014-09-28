(ns suricatta.types
  "High level sql toolkit for Clojure"
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

(defn context
  "Context instance constructor."
  ([^Connection conn ^Configuration conf]
     (Context. conn conf true))
  ([^Connection conn ^Configuration conf ^Boolean closeable]
     (Context. conn conf closeable)))

(defn context?
  [ctx]
  (instance? Context ctx))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query and QueryResult Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord Query [^org.jooq.Query q
                  ^org.jooq.Configuration conf]
  proto/IContext
  (get-context [_] (DSL/using conf))
  (get-configuration [_] conf)

  proto/IQuery
  (query [self _] self))

(defrecord ResultQuery [^org.jooq.ResultQuery q
                        ^org.jooq.Configuration conf]
  proto/IContext
  (get-context [_] (DSL/using conf))
  (get-configuration [_] conf)

  proto/IQuery
  (query [_ _] (Query. q conf)))

;; Constructors

(defn ^Query ->query
  "Default constructor for Query."
  [q conf]
  (Query. q conf))

(defn ^ResultQuery ->result-query
  "Default constructor for ResultQuery."
  [q conf]
  (ResultQuery. q conf))

;; Predicates

(defn query?
  [q]
  (instance? Query q))

(defn result-query?
  [q]
  (instance? ResultQuery q))

