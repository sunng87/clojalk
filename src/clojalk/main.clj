(ns clojalk.main
  (:gen-class)
  (:refer-clojure :exclude [use peek])  
  (:use [clojalk net core utils jmx wal])
  (:use [clojure.contrib.properties]))

(defn property [properties key]
  (.getProperty properties key))

(defn -main [& args]
  (let [prop-file-name (or (first args) "clojalk.properties")
        props (read-properties prop-file-name)]
    (binding [*clojalk-log-dir* (property props "wal.dir")
              *clojalk-log-count* (property props "wal.files")]
      (start-wal))
    (start-tasks)
    (binding [*clojalk-port* (as-int (property props "server.port"))]
      (start-server))
    (start-jmx-server)
    (println (str "Clojalk server started, listening on " 
                  (property props "server.port")))))
