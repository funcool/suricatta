(defproject suricatta "0.1.0-SNAPSHOT"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "http://example.com/FIXME"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.txt"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.jooq/jooq "3.4.2"]
                 [clojure.jdbc "0.3.0-SNAPSHOT"]
                 [postgresql "9.3-1101.jdbc41"]
                 [com.h2database/h2 "1.3.176"]])
