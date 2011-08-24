(ns clojalk.utils
  (:require [clojure.contrib.logging :as logging])
  (:import [java.util.concurrent Executors TimeUnit]))

(defn current-time []
  (System/currentTimeMillis))

(defmacro dbg [x]
  `(let [x# ~x]
    (println "dbg:" '~x "=" x#)
    x#))

;;------ utility functions ------------
(defn not-nil [x]
  (not (nil? x)))

(defn third [x]
  (nth x 2))

(defn uppercase [#^String s] (.toUpperCase s))

(defn conj-all [x s]
  "conject a sequence s into x"
  (if-not (empty? s)
    (apply conj x s)
    x))

(defn disj-all [x s]
  "disjoin a sequence from x"
  (if-not (empty? s)
    (apply disj x s)
    x))

(defn as-int [s]
  (Integer/valueOf s))

(defn remove-item [s i]
  (remove (fn [x] (= x i)) s))

;;------- scheduler ------------------

(defn- wrap-task [task]
  (try task (catch Exception e (logging/warn "Exception caught on scheduled task" e))))

(defn schedule-task [thread-pool-size & taskdefs]
  (let [scheduler (. Executors newScheduledThreadPool thread-pool-size)]
    (doseq [[task delay interval] taskdefs]
      (.scheduleWithFixedDelay scheduler (wrap-task task) delay interval (. TimeUnit SECONDS)))
    scheduler))
