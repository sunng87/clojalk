(ns clojalk.net
  (:refer-clojure :exclude [use peek])
  (:use [clojalk.core])
  (:use [clojalk.net.protocol])
  (:use [aleph.tcp])
  (:use [lamina.core])
  (:use [gloss.core]))

(defn echo-handler [ch client-info]
  (receive-all ch
    #(if-let [msg %]
       (do
         (println msg)
         (if (seq? msg) ;; known command will be transformed into a sequence by codec
           (case (first msg)
             "quit" (close ch)
             (enqueue ch (pr-str msg)))
         
           (enqueue ch "UNKNOWN_COMMAND"))))))

;; all sessions
(defonce sessions (ref {}))

;; 
(defn handle-command [ch cmd args])

(defn command-dispatcher [ch client-info]
  (receive-all ch
    #(if-let [msg %]
       (if (seq? msg)
         (handle-command ch (first msg) (rest msg))
         (enqueue ch "UNKNOWN_COMMAND")))))

(defn start-server [port]
  (start-tcp-server echo-handler {:port port, :frame beanstalkd-codec}))

(defn -main []
  (do
    (start-server 10000)
    (println "server started")))
