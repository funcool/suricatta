(defproject funcool/suricatta "1.0.0"
  :description "High level sql toolkit for clojure (backed by jooq library)"
  :url "https://github.com/funcool/suricatta"
  :license {:name "BSD (2-Clause)"
            :url "http://opensource.org/licenses/BSD-2-Clause"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha3" :scope "provided"]
                 [org.jooq/jooq "3.8.1"]]
  :javac-options ["-target" "1.8" "-source" "1.8" "-Xlint:-options"]
  :profiles
  {:dev {:global-vars {*warn-on-reflection* false}
         :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
         :aliases {"test-all"
                   ["with-profile" "dev,1.8:dev,1.7:dev,1.6:dev,1.5:dev" "test"]}
         :plugins [[lein-ancient "0.6.10"]]
         :dependencies [[org.postgresql/postgresql "9.4.1208"]
                        [com.h2database/h2 "1.4.192"]
                        [cheshire "5.6.1"]]}
   :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}}

  :java-source-paths ["src/java"])

