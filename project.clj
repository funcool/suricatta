(defproject suricatta "0.1.0-alpha"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/niwibe/suricatta"

  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}

  :profiles {:dev {:dependencies [[postgresql "9.3-1102.jdbc41"]
                                  [com.h2database/h2 "1.3.176"]
                                  [cheshire "5.3.1"]]
                   :global-vars {*warn-on-reflection* true}}}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.jooq/jooq "3.4.4"]
                 [clojure.jdbc "0.3.0"]
                 [cats "0.2.0" :exclusions [org.clojure/clojure]]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]])
