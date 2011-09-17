(ns clojalk.script.put
  (:refer-clojure :exclude [use peek read])
  (:use [beanstalk.core])
  (:use [clojure.contrib.properties]))

(if-not (= (count *command-line-args*) 3)
  (do
    (println "Usage: lein exec scripts/put.clj tube-name job-count")
    (System/exit 1)))

(def props (read-properties "./clojalk.properties"))
(def client (new-beanstalk (Integer/valueOf (.getProperty props "server.port"))))

(def job-body "<UserRequest><email>sunng@about.me</email><name>Sun Ning</name></UserRequest>")

(defn byte-length [s]
  (alength (.getBytes s "utf8")))

(let [args (rest *command-line-args*)
      tube-name (first args)
      job-count (Integer/valueOf (second args))]
  (use client tube-name)
  (loop [i 0]
    (if-not (= i job-count)
      (do
        (put client 500 0 1000 (byte-length job-body ) job-body)
        (recur (inc i))))))
