(defproject funcool/suricatta "0.6.1"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/funcool/suricatta"
  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}
  :dependencies [[org.clojure/clojure "1.7.0" :scope "provided"]
                 [org.jooq/jooq "3.7.2"]]
  ;; :jvm-opts ^:replace ["-Dclojure.compiler.direct-linking=true"]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :profiles
  {:dev {:global-vars {*warn-on-reflection* false}
         :aliases {"test-all" ["with-profile" "dev,1.8:dev,1.6:dev,1.5:dev" "test"]}
         :plugins [[lein-ancient "0.6.7"]]
         :dependencies [[org.postgresql/postgresql "9.4-1204-jdbc42"]
                        [com.h2database/h2 "1.4.190"]
                        [cheshire "5.5.0"]]}
   :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.8 {:dependencies [[org.clojure/clojure "1.8.0-RC4"]]}}

  :java-source-paths ["src/java"])

