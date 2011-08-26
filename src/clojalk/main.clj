(ns clojalk.main
  (:gen-class)
  (:refer-clojure :exclude [use peek])  
  (:use [clojalk net core utils]))

(defn -main []
  (do
    (start-tasks)
    (start-server 10000)
    (println "Clojalk server started")))
