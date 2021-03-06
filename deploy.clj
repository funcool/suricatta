(require '[clojure.java.shell :as shell]
         '[clojure.main])
(require '[badigeon.jar]
         '[badigeon.deploy])

(defmulti task first)

(defmethod task "jar"
  [args]
  (badigeon.jar/jar 'funcool/suricatta
                    {:mvn/version "2.0.0-SNAPSHOT"}
                    {:out-path "target/suricatta.jar"
                     :mvn/repos '{"clojars" {:url "https://repo.clojars.org/"}}
                     :allow-all-dependencies? false}))

(defmethod task "deploy"
  [args]
  (let [artifacts [{:file-path "target/suricatta.jar" :extension "jar"}
                   {:file-path "pom.xml" :extension "pom"}]]
    (badigeon.deploy/deploy
     'funcool/suricatta "2.0.0-SNAPSHOT"
     artifacts
     {:id "clojars" :url "https://repo.clojars.org/"}
     {:allow-unsigned? true})))


(defmethod task :default
  [args]
  (task ["jar"])
  (task ["deploy"]))

;;; Build script entrypoint. This should be the last expression.

(task *command-line-args*)
