(ns clojalk.net
  (:refer-clojure :exclude [use peek])
  (:require [clojure.contrib.logging :as logging])
  (:require [clojure.contrib.string :as string])
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
             (enqueue ch ["INSERTED" "5"]))
         
           (enqueue ch ["UNKNOWN_COMMAND"]))))))

;; sessions 
(defonce sessions (ref {}))

(defn get-or-create-session [ch remote-addr type]
  (dosync
    (if-not (contains? @sessions remote-addr)
      (let [new-session (open-session remote-addr type)]
        (alter new-session assoc :channel ch)
        (alter sessions assoc remote-addr new-session))))
  (@sessions remote-addr))

(defn close-session [remote-addr]
  (dosync
    (alter sessions dissoc remote-addr)))

;; reserve watcher
(defn reserve-watcher [key identity old-value new-value]
  (let [old-job (:incoming_job old-value)
        new-job (:incoming_job new-value)]
    (if (and new-job (not (= old-job new-job)))
      (let [ch (:channel new-value)]
        (enqueue ch ["RESERVED" (str (:id new-job)) (:body new-job)]))))
  (let [old-state (:state old-value)
        new-state (:state new-value)]
    (if (and (= :waiting old-state) (= :idle new-state))
      (enqueue (:channel new-value) ["TIMED_OUT"]))))

;; server handlers
(defn on-put [ch session args]
  (try
    (let [priority (as-int (first args))
          delay (as-int (second args))
          ttr (as-int (third args))
          body (last args)
          job (put session priority delay ttr body)]
      (if job
        (enqueue ch ["INSERTED" (str (:id job))])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-reserve [ch session]
  (add-watch session (:id session) reserve-watcher)
  (reserve session))

(defn on-use [ch session args]
  (let [tube-name (first args)]
    (use session tube-name)
    (enqueue ch ["USING" tube-name])))

(defn on-watch [ch session args]
  (let [tube-name (first args)]
    (watch session tube-name)
    (enqueue ch ["WATCHING" (str (count (:watch @session)))])))

(defn on-ignore [ch session args]
  (if (> (count (:watch @session)) 1)
    (let [tube-name (first args)]
      (ignore session tube-name)
      (enqueue ch ["WATCHING" (str (count (:watch @session)))]))
    (enqueue ch ["NOT_IGNORED"])))

(defn on-quit [ch remote-addr]
  (close-session remote-addr)
  (close ch))

(defn on-list-tubes [ch]
  (let [tubes (list-tubes nil)]
    (enqueue ch ["OK" (string/join "\r\n" (map string/as-str tubes))])))

(defn on-list-tube-used [ch session]
  (let [tube (list-tube-used session)]
    (enqueue ch ["USING" (string/as-str tube)])))

(defn on-list-tubes-watched [ch session]
  (let [tubes (list-tubes-watched session)]
    (enqueue ch ["OK" (string/join "\r\n" (map string/as-str tubes))])))

(defn on-release [ch session args]
  (try
    (let [id (as-int (first args))
          priority (as-int (second args))
          delay (as-int (third args))
          job (release session id priority delay)]
      (if (nil? job)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["RELEASED"])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-delete [ch session args]
  (try
    (let [id (as-int (first args))
          job (delete session id)]
      (if (nil? job)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["DELETED"])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-bury [ch session args]
  (try
    (let [id (as-int (first args))
          priority (as-int (second args))
          job (bury session id priority)]
      (if (nil? job)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["BURIED"])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-kick [ch session args]
  (try
    (let [bound (as-int (first args))
          jobs-kicked (kick session bound)]
      (enqueue ch ["KICKED" (str (count jobs-kicked))]))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-touch [ch session args]
  (try
    (let [id (as-int (first args))
          job (touch session id)]
      (if (nil? job)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["TOUCHED"])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-peek [ch session args]
  (try
    (let [id (as-int (first args))
          job (peek session id)]
      (if (nil? job)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["FOUND" (str (:id job)) (:body job)])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn- peek-job [ch session func]
  (let [job (func session)]
    (if (nil? job)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["FOUND" (str (:id job)) (:body job)]))))

(defn on-peek-ready [ch session]
  (peek-job ch session peek-ready))

(defn on-peek-delayed [ch session]
  (peek-job ch session peek-delayed))

(defn on-peek-buried [ch session]
  (peek-job ch session peek-buried))

(defn on-reserve-with-timeout [ch session args]
  (try
    (let [timeout (as-int (first args))]
      (add-watch session (:id session) reserve-watcher)
      (reserve-with-timeout session timeout))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn on-stats-job [ch args]
  (try
    (let [id (as-int (first args))
          format-fn #(str (string/as-str (first %)) ": " (string/as-str (last %)))
          stats (stats-job nil id)]
      (if (nil? stats)
        (enqueue ch ["NOT_FOUND"])
        (enqueue ch ["OK" (string/join "\r\n" (map format-fn stats))])))
    (catch NumberFormatException e (enqueue ch ["BAD_FORMAT"]))))

(defn command-dispatcher [ch client-info msg]
  (let [remote-addr (:remote-addr client-info)
        cmd (first msg)
        args (rest msg)]
    (case cmd
      "PUT" (on-put ch (get-or-create-session ch remote-addr :producer) args)
      "RESERVE" (on-reserve ch (get-or-create-session ch remote-addr :worker))
      "USE" (on-use ch (get-or-create-session ch remote-addr :producer) args)
      "WATCH" (on-watch ch (get-or-create-session ch remote-addr :worker) args)
      "IGNORE" (on-ignore ch (get-or-create-session ch remote-addr :worker) args)
      "QUIT" (on-quit ch remote-addr)
      "LIST-TUBES" (on-list-tubes ch)
      "LIST-TUBE-USED" 
        (on-list-tube-used ch (get-or-create-session ch remote-addr :producer))
      "LIST-TUBES-WATCHED" 
        (on-list-tubes-watched ch (get-or-create-session ch remote-addr :worker))
      "RELEASE" (on-release ch (get-or-create-session ch remote-addr :worker) args)
      "DELETE" (on-delete ch (get-or-create-session ch remote-addr :worker) args)
      "BURY" (on-bury ch (get-or-create-session ch remote-addr :worker) args)
      "KICK" (on-kick ch (get-or-create-session ch remote-addr :producer) args)
      "TOUCH" (on-touch ch (get-or-create-session ch remote-addr :worker) args)
      "PEEK" (on-peek ch (get-or-create-session ch remote-addr :producer) args)
      "PEEK-READY" (on-peek-ready ch (get-or-create-session ch remote-addr :producer))
      "PEEK-DELAYED" 
        (on-peek-delayed ch (get-or-create-session ch remote-addr :producer))
      "PEEK-BURIED" 
        (on-peek-buried ch (get-or-create-session ch remote-addr :producer))
      "RESERVE-WITH-TIMEOUT"
        (on-reserve-with-timeout ch (get-or-create-session ch remote-addr :worker) args)
      "STATS-JOB" (on-stats-job ch args)
      (enqueue ch ["UNKNOWN_COMMAND"]))))

(defn default-handler [ch client-info]
  (receive-all ch
    #(if-let [msg %]
       (if (seq? msg)
         (try 
           (command-dispatcher ch client-info msg)
           (catch Exception e 
                  (do
                    (logging/warn (str "error on processing " msg) e)
                    (enqueue ch ["INTERNAL_ERROR"]))))
         (enqueue ch ["UNKNOWN_COMMAND"])))))

(defn start-server [port]
  (start-tcp-server default-handler {:port port, :frame beanstalkd-codec}))

(defn -main []
  (do
    (start-tasks)
    (start-server 10000)
    (println "Clojalk server started")))
