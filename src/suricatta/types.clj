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

;; Insitu implementation for performance reasons.

(defn- keywordize-keys
  "Recursively transforms all map keys from strings to keywords."
  [m]
  (into {} (map (fn [[k v]] [(keyword (.toLowerCase k)) v]) m)))

(defn- default-record->map
  [^org.jooq.Record record]
  (keywordize-keys (.intoMap record)))

(defn- fetch-result-query-impl
  [^org.jooq.Query query ^org.jooq.DSLContext ctx
   {:keys [mapfn] :or {mapfn default-record->map}}]
  (let [result (.fetch ctx query)]
    (mapv mapfn result)))

(deftype Query [^org.jooq.Query q
                ^org.jooq.Configuration conf]
  proto/IContext
  (get-context [_] (DSL/using conf))

  proto/IQuery
  (query [self _] self)

  proto/IExecute
  (execute [self _]
    (let [ctx (DSL/using conf)]
      (.execute ctx q)))

  Object
  (equals [self other]
    (= q (.-q other)))

  (toString [_]
    (with-out-str (print [q (.dialect conf)]))))


(deftype ResultQuery [^org.jooq.ResultQuery q
                      ^org.jooq.Configuration conf]
  proto/IContext
  (get-context [_] (DSL/using conf))

  proto/IQuery
  (query [_ _] (Query. q conf))

  proto/IFetch
  (fetch [self _ opts]
    (let [ctx (DSL/using conf)]
      (fetch-result-query-impl q ctx opts)))

  Object
  (equals [self other]
    (= q (.-q other)))

  (toString [_]
    (with-out-str (print [q (.dialect conf)]))))

;; Predicates

(defn query?
  [q]
  (instance? Query q))

(defn result-query?
  [q]
  (instance? ResultQuery q))

