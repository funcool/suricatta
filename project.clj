(defproject suricatta "0.2.1"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/niwibe/suricatta"
  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.jooq/jooq "3.5.1"]
                 [cats "0.2.0" :exclusions [org.clojure/clojure]]
                 [clojure.jdbc "0.4.0-beta1"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]

  :javac-options ["-target" "1.7" "-source" "1.7" "-Xlint:-options"]
  :profiles {:dev {:global-vars {*warn-on-reflection* true
                                 *unchecked-math* :warn-on-boxed}
                   :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                                  [postgresql "9.3-1102.jdbc41"]
                                  [com.h2database/h2 "1.3.176"]
                                  [cheshire "5.3.1"]]}}
  :java-source-paths ["src/java"])

