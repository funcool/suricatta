(require '[suricatta.core :as sc])

(def dbspec {:subprotocol "postgresql"
             :subname "//127.0.0.1/test"})

(def sql "select x from generate_series(1, 1000000) as x")

;; (with-open [conn (sc/context dbspec)]
;;   (sc/atomic conn
;;     (let [result (sc/fetch conn sql)]
;;       (reduce (fn [acc item]
;;                 (prn "==>" item)
;;                 (Thread/sleep 5)
;;                 (inc acc))
;;               0
;;               result))))

(with-open [conn (sc/context dbspec)]
  (sc/atomic conn
    (with-open [cursor (sc/fetch-lazy conn sql)]
      (reduce (fn [acc item]
                (prn "==>" item)
                (Thread/sleep 5)
                (inc acc))
              0
              (sc/cursor->seq cursor)))))
