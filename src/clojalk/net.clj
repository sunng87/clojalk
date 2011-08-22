(ns clojalk.net
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core utils])
  (:use [clojalk.net.protocol])
  (:use [aleph.tcp])
  (:use [lamina.core])
  (:use [gloss.core]))

;; this is for test and debug only
(defn echo-handler [ch client-info]
  (receive-all ch
    #(if-let [msg %]
       (do
         (println msg)
         (if (seq? msg) ;; known command will be transformed into a sequence by codec
           (case (first msg)
             "quit" (close ch)
             (enqueue ch ["INSERTED" 5]))
         
           (enqueue ch "UNKNOWN_COMMAND"))))))

;; sessions 
(defonce sessions (ref {}))

(defn get-or-create-session [ch remote-addr type]
  (dosync
    (if-not (contains? @sessions remote-addr)
      (let [new-session (open-session type)]
        (alter new-session assoc :id remote-addr :channel ch)
        (alter sessions assoc remote-addr new-session))))
  (@sessions remote-addr))

(defn close-session [remote-addr]
  (dosync
    (alter sessions dissoc remote-addr)))

;; reserve watcher
(defn incoming-job-watcher [key identity old-value new-value]
  (let [old-job (:incoming_job old-value)
        new-job (:incoming_job new-value)]
    (if (and new-job (not (= old-job new-job)))
      (let [ch (:channel new-value)]
        (enqueue ch ["RESERVED" (:id new-job) (:body new-job)])))))

;; server handlers
(defn on-put [ch session args]
  (try
    (let [priority (as-int (first args))
          delay (as-int (second args))
          ttr (as-int (nth args 2))
          body (last args)
          job (put session priority delay ttr body)]
      (if job
        (enqueue ch ["INSERTED" (:id job)])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-reserve [ch session]
  (add-watch session (:id session) incoming-job-watcher)
  (reserve session))

(defn command-dispatcher [ch client-info cmd args]
  (let [remote-addr (:remote-addr client-info)]
    (case cmd
      "put" (on-put ch (get-or-create-session ch remote-addr :producer) args)
      "reserve" (on-reserve ch (get-or-create-session ch remote-addr :worker)))))

(defn default-handler [ch client-info]
  (receive-all ch
    #(if-let [msg %]
       (if (seq? msg)
         (try 
           (command-dispatcher ch client-info (first msg) (rest msg))
           (catch Exception e (enqueue ch ["INTERNAL_ERROR"])))
         (enqueue ch ["UNKNOWN_COMMAND"])))))

(defn start-server [port]
  (start-tcp-server echo-handler {:port port, :frame beanstalkd-codec}))

(defn -main []
  (do
    (start-server 10000)
    (println "server started")))
