(ns clojalk.utils
  (:require [clojure.contrib.logging :as logging])
  (:require [clojure.contrib.string :as string])
  (:import [java.util UUID])
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

(defn assoc-all [x s]
  "assoc a sequence of k-v pairs into x"
  (if-not (empty? s)
    (apply assoc x s)
    x))

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

(defn as-long [s]
  (Long/valueOf s))

(defn remove-item [s i]
  (remove (fn [x] (= x i)) s))

(defn uuid []
  (.toString (UUID/randomUUID)))

(defn line-based-string [x]
  (str "- " (string/as-str x) "\n"))

(defn format-coll [x]
  (let [sorted-coll (sort x)]
    (str "---\n"
         (string/join ""
                      (map line-based-string x)))))

(defn format-stats [x]
  (let [stats-keys (sort (keys x))]
    (str "---\n" 
         (string/join "" 
                      (map #(str (string/as-str %) ": " (string/as-str (x %)) "\n") stats-keys)))))

(def into-string-array (partial into-array String))

;;------- scheduler ------------------

(defn- wrap-task [task]
  (try task (catch Exception e (logging/warn "Exception caught on scheduled task" e))))

;; define scheduler as an agent so we can use it within an stm
(defonce compute-intensive-scheduler
  (agent (Executors/newScheduledThreadPool  (* 2 (.availableProcessors (Runtime/getRuntime))))))

;; schedule a delayed task to thread pool and return thread pool itself
(defn- do-schedule [threads task delay]
  (.schedule threads ^Runnable task ^long (long delay) ^TimeUnit TimeUnit/SECONDS)
  threads)

;; schedule a delayed task, can be used within an stm
(defn schedule [task delay]
  (send compute-intensive-scheduler do-schedule task delay))

