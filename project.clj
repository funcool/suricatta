(defproject funcool/suricatta "0.4.0-SNAPSHOT"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/funcool/suricatta"
  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}

  :dependencies [[org.clojure/clojure "1.7.0" :scope "provided"]
                 [org.jooq/jooq "3.6.2"]
                 [funcool/cats "1.0.0-SNAPSHOT"]
                 [funcool/clojure.jdbc "0.6.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]

  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[lein-ancient "0.6.7"]]
                   :dependencies [[postgresql "9.3-1102.jdbc41"]
                                  [com.h2database/h2 "1.4.188"]
                                  [cheshire "5.5.0"]]}}
  :java-source-paths ["src/java"])

