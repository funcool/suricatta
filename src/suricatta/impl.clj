;; Copyright (c) 2014-2019 Andrey Antukh <niwi@niwi.nz>
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
  (:require
   [clojure.string :as str]
   [clojure.walk :as walk]
   [suricatta.proto :as proto])
  (:import
   clojure.lang.PersistentVector
   java.sql.Connection
   java.sql.DriverManager
   java.sql.PreparedStatement
   java.util.Properties
   javax.sql.DataSource
   org.jooq.Configuration
   org.jooq.ConnectionProvider
   org.jooq.Cursor
   org.jooq.DSLContext
   org.jooq.DataType
   org.jooq.Field
   org.jooq.Param
   org.jooq.Query
   org.jooq.QueryPart
   org.jooq.Result
   org.jooq.ResultQuery
   org.jooq.SQLDialect
   org.jooq.TransactionContext
   org.jooq.TransactionProvider
   org.jooq.impl.DSL
   org.jooq.impl.DefaultConfiguration
   org.jooq.impl.DefaultTransactionContext
   org.jooq.tools.jdbc.JDBCUtils
   org.jooq.exception.DataAccessException
   org.jooq.util.mariadb.MariaDBDataType
   org.jooq.util.mysql.MySQLDataType
   org.jooq.util.postgres.PostgresDataType))

(set! *warn-on-reflection* false)

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

;; Default implementation for avoid call `satisfies?`

(extend-protocol proto/IParam
  Object
  (-param [v _] v)

  nil
  (-param [v _] v))

(defn wrap-if-need
  [ctx obj]
  (proto/-param obj ctx))

(defn- map->properties
  "Convert hash-map to java.utils.Properties instance. This method is used
  internally for convert dbspec map to properties instance, but it can
  be usefull for other purposes."
  [data]
  (let [p (Properties.)]
    (dorun (map (fn [[k v]] (.setProperty p (name k) (str v))) (seq data)))
    p))

(defn sql->param
  [sql & parts]
  (let [wrap (fn [o] (if (instance? Param o) o (DSL/val o)))
        parts (->> (map wrap parts) (into-array QueryPart))]
    (DSL/field ^String sql ^"[Lorg.jooq.QueryPart;" parts)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Connection management
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- make-connection
  [uri opts]
  (let [^Connection conn (proto/-connection uri opts)]
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

(defn- map->properties
  ^java.util.Properties
  [opts]
  (letfn [(reduce-fn [^Properties acc k v]
            (.setProperty acc (name k) (str v))
            acc)]
    (reduce-kv reduce-fn (Properties.) opts)))

(defn make-context
  ([conf] (make-context conf nil))
  ([conf conn]
   (reify
     proto/IContextHolder
     (-context [_] (DSL/using conf))
     (-config [_] conf)

     java.io.Closeable
     (close [_]
       (when (and conn (not (.isClosed conn)))
         (.close conn)
         (.set conf (org.jooq.impl.NoConnectionProvider.)))))))

(defn context
  [uri opts]
  (let [^Connection connection (make-connection uri opts)
        ^SQLDialect dialect (if (:dialect opts)
                               (translate-dialect (:dialect opts))
                               (JDBCUtils/dialect connection))
        ^Configuration conf (doto (DefaultConfiguration.)
                              (.set dialect)
                              (.set connection))]
    (make-context conf connection)))

(extend-protocol proto/IConnectionFactory
  java.sql.Connection
  (-connection [it opts] it)

  javax.sql.DataSource
  (-connection [it opts]
    (.getConnection it))

  java.lang.String
  (-connection [url opts]
    (let [url (if (.startsWith url "jdbc:") url (str "jdbc:" url))]
      (DriverManager/getConnection url (map->properties opts)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IExecute implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- make-params
  ^"[Ljava.lang.Object;"
  [^DSLContext context params]
  (->> (map (partial wrap-if-need context) params)
       (into-array Object)))

(extend-protocol proto/IExecute
  java.lang.String
  (-execute [^String sql ctx]
    (let [^DSLContext context (proto/-context ctx)]
      (.execute context sql)))

  org.jooq.Query
  (-execute [^Query query ctx]
    (let [^DSLContext context (proto/-context ctx)]
      (.execute context query)))

  PersistentVector
  (-execute [^PersistentVector sqlvec ctx]
    (let [^DSLContext context (proto/-context ctx)
          ^String sql (first sqlvec)
          params (make-params context (rest sqlvec))
          query (.query context sql params)]
      (.execute context ^Query query)))

  ResultQuery
  (-execute [query ctx]
    (let [^DSLContext context (proto/-context ctx)]
      (.execute context query))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetch Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; Default implementation for avoid call to `satisfies?`
(extend-protocol proto/ISQLType
  Object
  (-convert [v] v)

  nil
  (-convert [v] v))


(defn- result-record->record
  [^org.jooq.Record record]
  (letfn [(reduce-fn [acc ^Field field]
            (let [value (.getValue field record)
                  name (.getName field)]
              (assoc! acc (keyword (.toLowerCase name))
                      (proto/-convert value))))]
    (-> (reduce reduce-fn (transient {}) (.fields record))
        (persistent!))))

(defn- result-record->row
  [^org.jooq.Record record]
  (letfn [(reduce-fn [acc ^Field field]
            (let [value (.getValue field record)
                  name (.getName field)]
              (conj! acc (proto/-convert value))))]
    (-> (reduce reduce-fn (transient []) (.fields record))
        (persistent!))))

(defn- result->vector
  [^org.jooq.Result result {:keys [mapfn format] :or {rows false format :record}}]
  (if mapfn
    (mapv mapfn result)
    (case format
      :record (mapv result-record->record result)
      :row    (mapv result-record->row result)
      :json   (.formatJSON result)
      :csv    (.formatCSV result))))

(extend-protocol proto/IFetch
  String
  (-fetch [^String sql ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^Result result (.fetch context sql)]
      (result->vector result opts)))

  PersistentVector
  (-fetch [^PersistentVector sqlvec ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^String sql (first  sqlvec)
          params (make-params context (rest sqlvec))
          query (.resultQuery context sql params)]
      (-> (.fetch context ^ResultQuery query)
          (result->vector opts))))

  ResultQuery
  (-fetch [^ResultQuery query ctx opts]
    (let [^DSLContext context (proto/-context ctx)]
      (-> (.fetch context query)
          (result->vector opts)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IFetchLazy Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IFetchLazy
  java.lang.String
  (-fetch-lazy [^String query ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^ResultQuery query  (.resultQuery context query)]
      (->> (.fetchSize query (get opts :fetch-size 128))
           (.fetchLazy context))))

  PersistentVector
  (-fetch-lazy [^PersistentVector sqlvec ctx opts]
    (let [^DSLContext context (proto/-context ctx)
          ^String sql (first sqlvec)
          params (make-params context (rest sqlvec))
          query  (.resultQuery context sql params)]
      (->> (.fetchSize query (get opts :fetch-size 128))
           (.fetchLazy context))))

  org.jooq.ResultQuery
  (-fetch-lazy [^ResultQuery query ctx opts]
    (let [^DSLContext context (proto/-context ctx)]
      (->> (.fetchSize query (get opts :fetch-size 128))
           (.fetchLazy context)))))

(defn cursor->seq
  [^Cursor cursor {:keys [format mapfn] :or {format :record}}]
  (letfn [(transform-fn [item]
            (if mapfn
              (mapfn item)
              (case format
                :record (result-record->record item)
                :row (result-record->row item))))]
    (sequence (map transform-fn) cursor)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; IQuery Implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(extend-protocol proto/IQuery
  java.lang.String
  (-query [sql ctx]
    (let [^DSLContext context (proto/-context ctx)
          ^Configuration conf (proto/-config ctx)]
      (-> (.resultQuery context sql)
          (.keepStatement true))))

  PersistentVector
  (-query [sqlvec ctx]
    (let [^DSLContext context (proto/-context ctx)
          ^Configuration conf (proto/-config ctx)
          ^String sql (first sqlvec)
          params (make-params context (rest sqlvec))]
      (-> (.resultQuery context sql params)
          (.keepStatement true)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Load into implementation
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def ^{:doc "Datatypes translation map" :dynamic true}
  *datatypes*
  {:pg/varchar PostgresDataType/VARCHAR
   :pg/any PostgresDataType/ANY
   :pg/bigint PostgresDataType/BIGINT
   :pg/bigserial PostgresDataType/BIGSERIAL
   :pg/boolean PostgresDataType/BOOLEAN
   :pg/date PostgresDataType/DATE
   :pg/decimal PostgresDataType/DECIMAL
   :pg/real PostgresDataType/REAL
   :pg/double PostgresDataType/DOUBLEPRECISION
   :pg/int4 PostgresDataType/INT4
   :pg/int2 PostgresDataType/INT2
   :pg/int8 PostgresDataType/INT8
   :pg/integer PostgresDataType/INTEGER
   :pg/serial PostgresDataType/SERIAL
   :pg/serial4 PostgresDataType/SERIAL4
   :pg/serial8 PostgresDataType/SERIAL8
   :pg/smallint PostgresDataType/SMALLINT
   :pg/text PostgresDataType/TEXT
   :pg/time PostgresDataType/TIME
   :pg/timetz PostgresDataType/TIMETZ
   :pg/timestamp PostgresDataType/TIMESTAMP
   :pg/timestamptz PostgresDataType/TIMESTAMPTZ
   :pg/uuid PostgresDataType/UUID
   :pg/char PostgresDataType/CHAR
   :pg/bytea PostgresDataType/BYTEA
   :pg/numeric PostgresDataType/NUMERIC
   :pg/json PostgresDataType/JSON
   :maria/bigint MariaDBDataType/BIGINT
   :maria/ubigint MariaDBDataType/BIGINTUNSIGNED
   :maria/binary MariaDBDataType/BINARY
   :maria/blob MariaDBDataType/BLOB
   :maria/bool MariaDBDataType/BOOL
   :maria/boolean MariaDBDataType/BOOLEAN
   :maria/char MariaDBDataType/CHAR
   :maria/date MariaDBDataType/DATE
   :maria/datetime MariaDBDataType/DATETIME
   :maria/decimal MariaDBDataType/DECIMAL
   :maria/double MariaDBDataType/DOUBLE
   :maria/enum MariaDBDataType/ENUM
   :maria/float MariaDBDataType/FLOAT
   :maria/int MariaDBDataType/INT
   :maria/integer MariaDBDataType/INTEGER
   :maria/uint MariaDBDataType/INTEGERUNSIGNED
   :maria/longtext MariaDBDataType/LONGTEXT
   :maria/mediumint MariaDBDataType/MEDIUMINT
   :maria/real MariaDBDataType/REAL
   :maria/smallint MariaDBDataType/SMALLINT
   :maria/time MariaDBDataType/TIME
   :maria/timestamp MariaDBDataType/TIMESTAMP
   :maria/varchar MariaDBDataType/VARCHAR
   :mysql/bigint MySQLDataType/BIGINT
   :mysql/ubigint MySQLDataType/BIGINTUNSIGNED
   :mysql/binary MySQLDataType/BINARY
   :mysql/blob MySQLDataType/BLOB
   :mysql/bool MySQLDataType/BOOL
   :mysql/boolean MySQLDataType/BOOLEAN
   :mysql/char MySQLDataType/CHAR
   :mysql/date MySQLDataType/DATE
   :mysql/datetime MySQLDataType/DATETIME
   :mysql/decimal MySQLDataType/DECIMAL
   :mysql/double MySQLDataType/DOUBLE
   :mysql/enum MySQLDataType/ENUM
   :mysql/float MySQLDataType/FLOAT
   :mysql/int MySQLDataType/INT
   :mysql/integer MySQLDataType/INTEGER
   :mysql/uint MySQLDataType/INTEGERUNSIGNED
   :mysql/longtext MySQLDataType/LONGTEXT
   :mysql/mediumint MySQLDataType/MEDIUMINT
   :mysql/real MySQLDataType/REAL
   :mysql/smallint MySQLDataType/SMALLINT
   :mysql/time MySQLDataType/TIME
   :mysql/timestamp MySQLDataType/TIMESTAMP
   :mysql/varchar MySQLDataType/VARCHAR})

(defn typed-field
  [data type]
  (let [f (clojure.core/name data)
        dt (get *datatypes* type)]
    (DSL/field f ^DataType dt)))

(defn load-into
  [ctx tablename data {:keys [format commit fields ignore-rows
                              nullstring quotechar separator]
                       :or {format :csv commit :none ignore-rows 0
                            nullstring "" quotechar \" separator \,}}]
  (let [^DSLContext context (proto/-context ctx)
        step (.loadInto context (DSL/table (name tablename)))
        step (case commit
               :none (.commitNone step)
               :each (.commitEach step)
               :all  (.commitAll step)
               (.commitAfter step commit))
        step (case format
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Transactions
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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
      (let [result (apply func (make-context conf) args)
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
