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

(ns suricatta.impl
  (:require [suricatta.types :as types]
            [suricatta.proto :as proto]
            [clojure.string :as str]
            [clojure.walk :as walk])
  (:import org.jooq.impl.DSL
           org.jooq.impl.DefaultConfiguration
           org.jooq.conf.ParamType
           org.jooq.tools.jdbc.JDBCUtils
           org.jooq.SQLDialect
           org.jooq.DSLContext
           org.jooq.ResultQuery
           org.jooq.Query
           org.jooq.Field
           org.jooq.Result
           org.jooq.Cursor
           org.jooq.RenderContext
           org.jooq.BindContext
           org.jooq.Configuration
           clojure.lang.PersistentVector
           java.net.URI
           java.util.Properties
           java.sql.Connection
           java.sql.PreparedStatement
           java.sql.DriverManager
           javax.sql.DataSource
           suricatta.types.Context))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ^SQLDialect translate-dialect
  "Translate keyword dialect name to proper
  jooq SQLDialect enum value."
  [dialect]
  (if (instance? SQLDialect dialect)
    dialect
    (case dialect
      :postgresql SQLDialect/POSTGRES
      :postgres   SQLDialect/POSTGRES
      :pgsql      SQLDialect/POSTGRES
      :mariadb    SQLDialect/MARIADB
      :firebird   SQLDialect/FIREBIRD
      :mysql      SQLDialect/MYSQL
      :h2         SQLDialect/H2
      :sqlite     SQLDialect/SQLITE
      SQLDialect/SQL99)))

(def ^{:doc "Transaction isolation levels" :static true}
  +isolation-levels+
  {:none             Connection/TRANSACTION_NONE
   :read-uncommitted Connection/TRANSACTION_READ_UNCOMMITTED
   :read-committed   Connection/TRANSACTION_READ_COMMITTED
   :repeatable-read  Connection/TRANSACTION_REPEATABLE_READ
   :serializable     Connection/TRANSACTION_SERIALIZABLE})

(defn- render-inline?
  "Return true if the current render/bind context
  allow inline sql rendering.

  This function should be used on third party
  types/fields adapters."
  {:internal true}
  [^org.jooq.Context context]
  (let [^ParamType ptype (.paramType context)]
    (or (= ptype ParamType/INLINED)
        (= ptype ParamType/NAMED_OR_INLINED))))

(extend-protocol proto/IParamContext
  RenderContext
  (-statement [_] nil)
  (-next-bind-index [_] nil)
  (-inline? [it] (render-inline? it))

  BindContext
  (-statement [it] (.statement it))
  (-next-bind-index [it] (.nextIndex it))
  (-inline? [it] (render-inline? it)))

(def ^:private param-adapter
  (reify suricatta.impl.IParam
    (render [_ value ^RenderContext ctx]
      (when-let [sql (proto/-render value ctx)]
        (.sql ctx sql)))
    (bind [_ value ^BindContext ctx]
      (proto/-bind value ctx))))

(extend-protocol proto/IRenderer
  org.jooq.Query
  (-sql [q type dialect]
    (let [^Configuration conf (DefaultConfiguration.)
          ^DSLContext context (DSL/using conf)]
      (when dialect
        (.set conf (translate-dialect dialect)))
      (condp = type
        nil      (.render context q)
        :named   (.renderNamedParams context q)
        :indexed (.render context q)
        :inlined (.renderInlined context q))))

  (-bind-values [q]
    (let [^Configuration conf (DefaultConfiguration.)
          ^DSLContext context (DSL/using conf)]
      (into [] (.extractBindValues context q))))

  suricatta.types.Deferred
  (-sql [self type dialect]
    (proto/-sql @self type dialect))

  (-bind-values [self]
    (proto/-bind-values @self)))

(defn make-param-impl
  "Wraps a value that implements IParamType
  protocol in valid jOOQ Param implementation."
  [value]
  (suricatta.impl.ParamWrapper. param-adapter value))

(defn wrap-if-need
  [obj]
  (if (satisfies? proto/IParamType obj)
    (make-param-impl obj)
    obj))

(defn- querystring->map
  "Given a URI instance, return its querystring as
  plain map with parsed keys and values."
  [^URI uri]
  (let [^String query (.getQuery uri)]
    (->> (for [^String kvs (.split query "&")] (into [] (.split kvs "=")))
         (into {})
         (walk/keywordize-keys))))

(defn- map->properties
  "Convert hash-map to java.utils.Properties instance. This method is used
  internally for convert dbspec map to properties instance, but it can
  be usefull for other purposes."
  [data]
  (let [p (Properties.)]
    (dorun (map (fn [[k v]] (.setProperty p (name k) (str v))) (seq data)))
    p))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn make-connection
  [dbspec opts]
  (let [^Connection conn (proto/-connection dbspec)
        opts (merge (when (map? dbspec) dbspec) opts)]

    ;; Set readonly flag if it found on the options map
    (some->> (:read-only opts)
             (.setReadOnly conn))

    ;; Set the concrete isolation level if it found
    ;; on the options map
    (some->> (:isolation-level opts)
             (get +isolation-levels+)
             (.setTransactionIsolation conn))

    ;; Set the schema if it found on the options map
    (some->> (:schema opts)
             (.setSchema conn))

    conn))

(declare uri->dbspec)
(declare dbspec->connection)

(extend-protocol proto/IConnectionFactory
  java.sql.Connection
  (-connection [it] it)

  javax.sql.DataSource
  (-connection [it]
    (.getConnection it))

  clojure.lang.IPersistentMap
  (-connection [dbspec]
    (dbspec->connection dbspec))

  java.net.URI
  (-connection [uri]
    (-> (uri->dbspec uri)
        (dbspec->connection)))

  java.lang.String
  (-connection [uri]
    (let [uri (URI. uri)]
      (proto/-connection uri))))

(defn dbspec->connection
  "Create a connection instance from dbspec."
  [{:keys [subprotocol subname user password
           name vendor host port datasource classname]
    :as dbspec}]
  (cond
    (and name vendor)
    (let [host   (or host "127.0.0.1")
          port   (if port (str ":" port) "")
          dbspec (-> (dissoc dbspec :name :vendor :host :port)
                     (assoc :subprotocol vendor
                            :subname (str "//" host port "/" name)))]
      (dbspec->connection dbspec))

    (and subprotocol subname)
    (let [url (format "jdbc:%s:%s" subprotocol subname)
          options (dissoc dbspec :subprotocol :subname)]

      (when classname
        (Class/forName classname))

      (DriverManager/getConnection url (map->properties options)))

    ;; NOTE: only for legacy dbspec format compatibility
    (and datasource)
    (proto/-connection datasource)

    :else
    (throw (IllegalArgumentException. "Invalid dbspec format"))))

(defn uri->dbspec
  "Parses a dbspec as uri into a plain dbspec. This function
  accepts `java.net.URI` or `String` as parameter."
  [^URI uri]
  (let [host (.getHost uri)
        port (.getPort uri)
        path (.getPath uri)
        scheme (.getScheme uri)
        userinfo (.getUserInfo uri)]
    (merge
      {:subname (if (pos? port)
                 (str "//" host ":" port path)
                 (str "//" host path))
       :subprotocol scheme}
      (when userinfo
        (let [[user password] (str/split userinfo #":")]
          {:user user :password password}))
      (querystring->map uri))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IExecute implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IExecute
  java.lang.String
  (-execute [^String sql ^Context ctx]
    (let [^DSLContext context (proto/-context ctx)]
      (.execute context sql)))

  org.jooq.Query
  (-execute [^Query query ^Context ctx]
    (let [^DSLContext context (proto/-context ctx)]
      (.execute context query)))

  clojure.lang.PersistentVector
  (-execute [^PersistentVector sqlvec ^Context ctx]
    (let [^DSLContext context (proto/-context ctx)
          ^Query query (->> (map wrap-if-need (rest sqlvec))
                            (into-array Object)
                            (.query context (first sqlvec)))]
      (.execute context query)))

  suricatta.types.Deferred
  (-execute [deferred ctx]
    (proto/-execute @deferred ctx))

  suricatta.types.Query
  (-execute [query ctx]
    (let [^DSLContext context (if (nil? ctx)
                                (proto/-context query)
                                (proto/-context ctx))
          ^ResultQuery query  (.-query query)]
      (.execute context query))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetch Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- result-record->record
  [^org.jooq.Record record]
  (into {} (for [^int i (range (.size record))]
             (let [^Field field (.field record i)
                   value (.getValue record i)]
               [(keyword (.toLowerCase (.getName field)))
                (if (satisfies? proto/ISQLType value)
                  (proto/-convert value)
                  value)]))))

(defn- result-record->row
  [^org.jooq.Record record]
  (into [] (for [^int i (range (.size record))]
             (let [value (.getValue record i)]
               (if (satisfies? proto/ISQLType value)
                  (proto/-convert value)
                  value)))))

(defn- result->vector
  [^org.jooq.Result result {:keys [mapfn into format]
                            :or {rows false format :record}}]
  (if mapfn
    (mapv mapfn result)
    (condp = format
      :record (mapv result-record->record result)
      :row    (mapv result-record->row result)
      :json   (if into
                (.formatJSON result into)
                (.formatJSON result))
      :csv    (if into
                (.formatCSV result into)
                (.formatCSV result)))))

(extend-protocol proto/IFetch
  String
  (-fetch [^String sql ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^Result result (.fetch context sql)]
      (result->vector result opts)))

  PersistentVector
  (-fetch [^PersistentVector sqlvec ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^ResultQuery query (->> (into-array Object (map wrap-if-need (rest sqlvec)))
                                  (.resultQuery context (first sqlvec)))]
      (-> (.fetch context query)
          (result->vector opts))))

  org.jooq.ResultQuery
  (-fetch [^ResultQuery query ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)]
      (-> (.fetch context query)
          (result->vector opts))))

  org.jooq.Query
  (-fetch [^Query query ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^Configuration config (proto/-config ctx)
          ^SQLDialect dialect (.dialect config)
          sqlvec (apply vector
                        (proto/-sql query :indexed dialect)
                        (proto/-bind-values query))]
      (proto/-fetch sqlvec ctx opts)))

  suricatta.types.Deferred
  (-fetch [deferred ctx opts]
    (proto/-fetch @deferred ctx opts))

  suricatta.types.Query
  (-fetch [query ctx opts]
    (let [^DSLContext context (if (nil? ctx)
                                (proto/-context query)
                                (proto/-context ctx))
          ^ResultQuery query  (.-query query)]
      (-> (.fetch context query)
          (result->vector opts)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetchLazy Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IFetchLazy
  java.lang.String
  (-fetch-lazy [^String query ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^ResultQuery query  (.resultQuery context query)]
      (.fetchSize query (get opts :fetch-size 60))
      (.fetchLazy context query)))

  clojure.lang.PersistentVector
  (-fetch-lazy [^PersistentVector sqlvec ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^ResultQuery query (->> (into-array Object (rest sqlvec))
                                  (.resultQuery context (first sqlvec)))]
      (.fetchSize query (get opts :fetch-size 100))
      (.fetchLazy context query)))

  org.jooq.ResultQuery
  (-fetch-lazy [^ResultQuery query ^Context ctx opts]
    (let [^DSLContext context (proto/-context ctx)]
      (.fetchSize query (get opts :fetch-size 100))
      (.fetchLazy context query)))

  suricatta.types.Deferred
  (-fetch-lazy [deferred ctx opts]
    (proto/-fetch-lazy @deferred ctx opts)))

(defn cursor->lazyseq
  [^Cursor cursor {:keys [format mapfn] :or {format :record}}]
  (let [lseq (fn thisfn []
               (when (.hasNext cursor)
                 (let [item (.fetchOne cursor)
                       record (condp = format
                                :record (result-record->record item)
                                :row (result-record->row item))]
                   (cons record (lazy-seq (thisfn))))))]
    (lseq)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IQuery Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IQuery
  java.lang.String
  (-query [sql ctx]
    (let [^DSLContext context (proto/-context ctx)
          ^Configuration conf (proto/-config ctx)
          ^ResultQuery query  (-> (.resultQuery context sql)
                                  (.keepStatement true))]
      (types/query query conf)))

  PersistentVector
  (-query [sqlvec ctx]
    (let [^DSLContext context (proto/-context ctx)
          ^Configuration conf (proto/-config ctx)
          ^ResultQuery query  (->> (into-array Object (rest sqlvec))
                                   (.resultQuery context (first sqlvec)))]
      (-> (doto query
            (.keepStatement true))
          (types/query conf)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load into implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn load-into
  [ctx tablename data {:keys [format commit fields ignore-rows
                              nullstring quotechar separator]
                       :or {format :csv commit :none ignore-rows 0
                            nullstring "" quotechar \" separator \,}}]
  (let [^DSLContext context (proto/-context ctx)
        step (.loadInto context (DSL/table (name tablename)))
        step (condp = commit
               :none (.commitNone step)
               :each (.commitEach step)
               :all  (.commitAll step)
               (.commitAfter step commit))
        step (condp = format
               :csv  (.loadCSV step data)
               :json (.loadJSON step data))
        fields (into-array org.jooq.Field fields)]
    (doto step
      (.fields fields)
      (.ignoreRows ignore-rows))
    (when (= format :csv)
      (doto step
        (.quote quotechar)
        (.nullString nullstring)
        (.separator separator)))
    (.execute step)))
