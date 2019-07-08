(require '[criterium.core :as b])
(require '[next.jdbc :as jdbc])
(require '[next.jdbc.result-set :as jdbc-rs])
(require '[suricatta.core :as sc])

(def dbspec1 {:subprotocol "postgresql"
              :subname "//127.0.0.1/test"})


(def dbspec2 {:dbtype "postgresql"
              :dbname "test"})

;; (def dbspec1 {:dbtype "h2:mem" :dbname "example1"})
;; (def dbspec2 {:subprotocol "h2" :subname "mem:example2"})

(def conn1 (jdbc/get-connection dbspec2))
(def conn2 (sc/context dbspec1))

;; (def sql "SELECT x FROM SYSTEM_RANGE(1, 1000);")
(def sql1 "SELECT x FROM generate_series(1, 1000) as x;")
(def sql2 "SELECT x FROM generate_series(1, 100000) as x;")

;; Test next.jdbc

(defn test-next-jdbc1
  []
  (let [result (jdbc/execute! conn1 [sql1] {:builder-fn jdbc-rs/as-unqualified-lower-maps})]
    (with-out-str
      (prn result))))

(defn test-suricatta1
  []
  (let [result (sc/fetch conn2 [sql1])]
    (with-out-str
      (prn result))))

(defn test-next-jdbc2
  []
  (jdbc/with-transaction [tx conn1]
    (let [cursor (jdbc/plan tx [sql2] {:builder-fn jdbc-rs/as-unqualified-lower-maps
                                       :fetch-size 128})
          result (reduce (fn [acc item]
                           (+ acc (:x item)))
                         0
                         cursor)]
      (with-out-str
        (prn "result:" result)))))

(defn test-suricatta2
  []
  (sc/atomic conn2
    (with-open [cursor (sc/fetch-lazy conn2 sql2)]
      (let [result (reduce (fn [acc item]
                             (+ acc (:x item)))
                           0
                           (sc/cursor->seq cursor))]
        (with-out-str
          (prn "result:" result))))))


;; (println "***** START: next.jdbc (1) *****")
;; ;; (b/with-progress-reporting (b/quick-bench (test-next-jdbc1) :verbose))
;; (b/quick-bench (test-next-jdbc1))
;; (println "***** END: next.jdbc (1) *****")

;; (println "***** START: suricatta (1) *****")
;; ;; (b/with-progress-reporting (b/quick-bench (test-suricatta1) :verbose))
;; (b/quick-bench (test-suricatta1))
;; (println "***** END: suricatta (1) *****")


;; (println "***** START: next.jdbc (2) *****")
;; ;; ;; (b/with-progress-reporting (b/quick-bench (test-next-jdbc1) :verbose))
;; (b/quick-bench (test-next-jdbc2))
;; (println "***** END: next.jdbc (2) *****")

(println "***** START: suricatta (2) *****")
;; (b/with-progress-reporting (b/quick-bench (test-suricatta1) :verbose))
(b/quick-bench (test-suricatta2))
(println "***** END: suricatta (2) *****")



;; (println (test-suricatta2))
;; (println (test-next-jdbc2))
