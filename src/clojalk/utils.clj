(ns clojalk.utils)

(defn current-time []
  (System/currentTimeMillis))

(defmacro dbg [x]
  `(let [x# ~x]
    (println "dbg:" '~x "=" x#)
    x#))

(defn not-nil [x]
  (not (nil? x)))