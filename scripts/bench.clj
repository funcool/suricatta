;; HOW TO RUN: clojure -J-Xmx128m -Adev:bench scripts/bench.clj

(require '[criterium.core :as b])
(require '[next.jdbc :as jdbc])
(require '[next.jdbc.result-set :as jdbc-rs])
(require '[suricatta.core :as sc])

(def uri "jdbc:postgresql://127.0.0.1/test")

(def conn1 (jdbc/get-connection uri))
(def conn2 (sc/context uri))

(def sql1 "SELECT x FROM generate_series(1, 1000) as x;")

(defn test-next-jdbc1
  []
  (let [result (jdbc/execute! conn1 [sql1] {:builder-fn jdbc-rs/as-unqualified-lower-maps})]
    (with-out-str
      (prn result))))

(defn test-suricatta1
  []
  (let [result (sc/fetch conn2 sql1)]
    (with-out-str
      (prn result))))

(println "***** START: next.jdbc (1) *****")
;; (b/with-progress-reporting (b/quick-bench (test-next-jdbc1) :verbose))
(b/quick-bench (test-next-jdbc1))
(println "***** END: next.jdbc (1) *****")

(println "***** START: suricatta (1) *****")
;; (b/with-progress-reporting (b/quick-bench (test-suricatta1) :verbose))
(b/quick-bench (test-suricatta1))
(println "***** END: suricatta (1) *****")


