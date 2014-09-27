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

(defprotocol IRenderer
  (get-sql [_ type dialect] "Render a query sql into a string.")
  (get-bind-values [_] "Get query bind values."))
