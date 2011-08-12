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
