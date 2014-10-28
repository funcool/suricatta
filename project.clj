(defproject suricatta "0.1.0-SNAPSHOT"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/niwibe/suricatta"

  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}

  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.jooq/jooq "3.4.4"]
                 [clojure.jdbc "0.3.0"]
                 [postgresql "9.3-1101.jdbc41"]
                 [com.h2database/h2 "1.3.176"]])
