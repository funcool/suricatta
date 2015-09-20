;; Copyright (c) 2014-2015 Andrey Antukh <niwi@niwi.nz>
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

(ns suricatta.async
  (:require [suricatta.core :as sc]
            [cats.monad.exception :as exc]
            [promissum.core :as p]))

(defn execute
  "Execute a query asynchronously returning a channel."
  ([ctx q]
   (execute ctx q {}))
  ([ctx q opts]
   (let [act (.-act ctx)
         fun #(% (exc/try-on (sc/execute ctx q)))]
     (p/promise
      (fn [deliver]
        (send-off act (fn [_] (fun deliver))))))))

(defn fetch
  "Execute a query asynchronously returning a channel."
  ([ctx q]
   (fetch ctx q {}))
  ([ctx q opts]
   (let [act (.-act ctx)
         fun #(% (exc/try-on (sc/fetch ctx q opts)))]
     (p/promise
      (fn [deliver]
        (send-off act (fn [_] (fun deliver))))))))
