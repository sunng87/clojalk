(ns clojalk.utils)

(defn current-time []
  (System/currentTimeMillis))

(defmacro dbg [x]
  `(let [x# ~x]
    (println "dbg:" '~x "=" x#)
    x#))

;;------ utility functions ------------
(defn not-nil [x]
  (not (nil? x)))

(defn uppercase [#^String s] (.toUpperCase s))

(defn conj-all [x s]
  "conject a sequence s into x"
  (apply conj x s))

(defn disj-all [x s]
  "disjoin a sequence from x"
  (apply disj x s))

(defn as-int [s]
  (Integer/valueOf s))