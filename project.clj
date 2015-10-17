(defproject funcool/suricatta "0.5.0"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/funcool/suricatta"
  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}
  :dependencies [[org.clojure/clojure "1.8.0-beta1" :scope "provided"]
                 [org.jooq/jooq "3.7.0"]]

  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[lein-ancient "0.6.7"]]
                   :dependencies [[org.postgresql/postgresql "9.4-1204-jdbc42"]
                                  [com.h2database/h2 "1.4.190"]
                                  [cheshire "5.5.0"]]}}
  :java-source-paths ["src/java"])

