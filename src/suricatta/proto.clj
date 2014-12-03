;; Copyright (c) 2014, Andrey Antukh
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

(defprotocol IFetchLazy
  (fetch-lazy [q ctx opts] "Fetch lazy results executing query."))

(defprotocol IRenderer
  (get-sql [_ type dialect] "Render a query sql into a string.")
  (get-bind-values [_] "Get query bind values."))

(defprotocol IQuery
  (query [_ ctx] "Build a query."))

;; Custom data types binding protocols

(defprotocol IParamType
  (render [_] "Render param value as inline sql")
  (bind [_ stmt index] "Bind param value to the statement."))
