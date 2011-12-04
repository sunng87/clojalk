(defproject clojalk "1.0.0-SNAPSHOT"
  :description "A beanstalkd clone in clojure"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [org.clojure/java.jmx "0.1"]
                    [aleph "0.2.1-SNAPSHOT"]]
  :dev-dependencies [[org.clojars.sunng/beanstalk "1.0.6"]
                     [lein-exec "0.1"]
                     [lein-marginalia "0.6.1"]]
  :warn-on-reflection true
  :main clojalk.main)
