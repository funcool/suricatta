(ns cljooq.types
  "High level sql toolkit for Clojure"
  (:require [clojure.walk :refer [postwalk]]
            [jdbc.core :as jdbc]
            [cljooq.proto :as proto])
  (:import org.jooq.impl.DSL
           org.jooq.SQLDialect))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Context
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftype Context [ctx]
  proto/IContext
  (get-context [self] ctx))

(defn context?
  [ctx]
  (instance? Context ctx))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query and QueryResult Types
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
                ^org.jooq.DSLContext ctx]
  proto/IContext
  (get-context [_] ctx)

  proto/IQuery
  (query [self _] self)

  proto/ISqlVector
  (get-sql [_ type]
    (case type
      :named (.getSQL q org.jooq.conf.ParamType/NAMED)
      :inlined (.getSQL q org.jooq.conf.ParamType/INLINED)
      :indexed (.getSQL q org.jooq.conf.ParamType/INDEXED)))

  (get-bind-values [_]
    (into [] (.getBindValues q)))

  (sqlvec [self]
    (apply vector
           (proto/get-sql self :indexed)
           (proto/get-bind-values self)))

  proto/IExecute
  (execute [_ _]
    (.execute ctx q))

  Object
  (equals [self other]
    (= q (.-q other)))

  (toString [_]
    (with-out-str (print [q]))))


(deftype ResultQuery [^org.jooq.ResultQuery q
                      ^org.jooq.DSLContext ctx]
  proto/IContext
  (get-context [_] ctx)

  proto/IQuery
  (query [_ _] (Query. q ctx))

  proto/IFetch
  (fetch [_ _ opts]
    (fetch-result-query-impl q ctx opts))

  Object
  (equals [self other]
    (= q (.-q other)))

  (toString [_]
    (with-out-str (print [q]))))

;; Predicates

(defn query?
  [q]
  (instance? Query q))

(defn result-query?
  [q]
  (instance? ResultQuery q))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Context Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IContextBuilder
  jdbc.types.Connection
  (make-context [conn]
    (let [jdbconn  (:connection conn)
          settings (doto (org.jooq.conf.Settings.)
                     (.setRenderNameStyle org.jooq.conf.RenderNameStyle/LOWER)
                     (.setRenderKeywordStyle org.jooq.conf.RenderKeywordStyle/LOWER))
          ctx       (DSL/using jdbconn settings)]
      (Context. ctx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IQuery
  java.lang.String
  (query [sql ctx]
    (Query. (.query ctx sql) ctx))

  clojure.lang.PersistentVector
  (query [sqlvec ctx]
    (let [sql    (first sqlvec)
          params (rest sqlvec)]
      (-> (.query ctx sql (into-array Object params))
          (Query. ctx)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Result Query Constructor Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IResultQuery
  java.lang.String
  (result-query [sql ctx opts]
    (ResultQuery. (.resultQuery ctx sql) ctx)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Convenience implementation for IExecute
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Convenience implementations for make easy executing
;; simple queries directly without explictly creating
;; a Query instance.

(extend-protocol proto/IExecute
  java.lang.String
  (execute [sql ctx]
    (.execute ctx sql))

  clojure.lang.PersistentVector
  (execute [sqlvec ctx]
    (let [query (proto/query sqlvec ctx)]
      (proto/execute query ctx))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Convenience implementation for ISqlVector
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/ISqlVector
  org.jooq.Query
  (get-sql [q type]
    (case type
      :named (.getSQL q org.jooq.conf.ParamType/NAMED)
      :inlined (.getSQL q org.jooq.conf.ParamType/INLINED)
      :indexed (.getSQL q org.jooq.conf.ParamType/INDEXED)))

  (get-bind-values [q]
    (into [] (.getBindValues q)))

  (sqlvec [q]
    (apply vector
           (proto/get-sql q :indexed)
           (proto/get-bind-values q))))
