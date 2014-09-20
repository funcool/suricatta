(ns cljooq.proto)

(defprotocol IContextBuilder
  (make-context [_] "Create new context (dslcontext)."))

(defprotocol IContext
  (get-context [_] "Get inner context"))

(defprotocol IExecute
  (execute [query ctx] "Execute a query and return a number of rows affected."))

(defprotocol IFetch
  (fetch [query ctx opts] "Fetch eagerly results executing query."))

(defprotocol IQuery
  (query [obj ctx] "Query constructor."))

(defprotocol IResultQuery
  (result-query [obj ctx opts] "ResultQuery constructor."))

(defprotocol ISqlVector
  (get-sql [_ type] "Get sql.")
  (get-bind-values [_] "Get bind values.")
  (sqlvec [_] "Get sql in sqlvector format."))
