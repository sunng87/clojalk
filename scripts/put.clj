(ns clojalk.script.put
  (:refer-clojure :exclude [use peek read])
  (:use [beanstalk.core])
  (:use [clojure.contrib.properties])
  (:import [java.util.concurrent CountDownLatch]))

(if-not (= (count *command-line-args*) 4)
  (do
    (println "Usage: lein exec scripts/put.clj tube-name job-count connection-count")
    (System/exit 1)))

(def props (read-properties "./clojalk.properties"))
(defn get-client []  (new-beanstalk (Integer/valueOf (.getProperty props "server.port"))))
;(def client (new-beanstalk 11300))

(def job-body "<UserRequest><email>sunng@about.me</email><name>Sun Ning</name></UserRequest>")

(def job-body-length
  (alength ^bytes (.getBytes ^String job-body "utf8")))

(def tube-name (nth *command-line-args* 1))
(def total-jobs (atom (Integer/valueOf (nth *command-line-args* 2))))
(def total-clients (Integer/valueOf (nth *command-line-args* 3)))
(def latch (CountDownLatch. total-clients))

(defn do-put-jobs []
  (let [client (get-client)]
    (use client tube-name)
    (loop []
      (if (<= @total-jobs 0)
        (.countDown latch)
        (do
          (put client (rand-int 2048) 0 1000 job-body-length job-body)
          (swap! total-jobs dec)
          (recur))))))

(defn run-in-thread [runnable]
  (let [t (Thread. runnable)]
    (.setDaemon t false)
    (.start t)))

(time
  (do
    (dorun (map run-in-thread (take total-clients (repeat do-put-jobs))))
    (.await latch)))

