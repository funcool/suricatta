(defproject funcool/suricatta "0.3.0"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/funcool/suricatta"
  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}
  :dependencies [[org.clojure/clojure "1.6.0" :scope "provided"]
                 [org.jooq/jooq "3.6.2"]
                 [cats "0.4.0" :exclusions [org.clojure/clojure]]
                 [funcool/clojure.jdbc "0.5.1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]

  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :dependencies [[org.clojure/clojure "1.7.0-beta2"]
                                  [postgresql "9.3-1102.jdbc41"]
                                  [com.h2database/h2 "1.4.187"]
                                  [cheshire "5.4.0"]]}}
  :java-source-paths ["src/java"])

