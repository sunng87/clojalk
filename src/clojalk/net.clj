(ns clojalk.net
  (:refer-clojure :exclude [use peek])
  (:use [clojalk.core])
  (:use [clojalk.net.protocol])
  (:use [aleph.tcp])
  (:use [lamina.core])
  (:use [gloss.core]))

(defn echo-handler [ch client-info]
  (receive-all ch
    #(let [msg %]
       (println msg)
       (case (first msg)
           "quit" (close ch)
           (enqueue ch (str "=>" (pr-str msg) "\r\n"))))))

(defn start-server [port]
  (start-tcp-server echo-handler {:port port, :frame beanstalkd-codec}))

(defn -main []
  (do
    (start-server 10000)
    (println "server started")))
