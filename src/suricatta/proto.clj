(ns suricatta.proto)

(defprotocol IContextBuilder
  (make-context [_] "Create new context (dslcontext)."))

(defprotocol IContext
  (get-context [_] "Get context with attached configuration")
  (get-configuration [_] "Get attached configuration."))

(defprotocol IExecute
  (execute [q ctx] "Execute a query and return a number of rows affected."))

(defprotocol IFetch
  (fetch [q ctx opts] "Fetch eagerly results executing query."))

(defprotocol IRenderer
  (get-sql [_ type dialect] "Render a query sql into a string.")
  (get-bind-values [_] "Get query bind values."))

(defprotocol IQuery
  (query [_ ctx] "Build a query."))
