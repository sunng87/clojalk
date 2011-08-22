(ns clojalk.net.protocol
  (:use [gloss.core])
  (:use [clojure.string :only [trim]]))

;; a wrapper for string-integer, copied from ztellman's aleph redis client
;; this codec adds an offset to string length, which is common seen in text
;; based protocol (message with \r\n as suffix)
(defn string-length-and-offset [count-offset]
  (prefix 
    (string-integer :ascii :delimiters ["\r\n"] :as-str true)
    #(if (neg? %) 0 (+ % count-offset))
    #(if-not % -1 (- % count-offset))))

;; --------- gloss codec definitions -----------
(defcodec token (string :ascii :delimiters [" " "\n" "\r\n"]))
(defcodec body 
  (finite-frame
     (string-length-and-offset 2)
     (string :utf8 :suffix "\r\n")))

(def codec-map 
  {"quit" []
   "reserve" [token]
   "put" [token token token body]})

(defn- commands-mapping [cmd]
  (if (contains? codec-map cmd)
    (compile-frame (codec-map cmd) nil #(cons cmd %))
    (string :utf8 :delimiters ["\r\n"])))

(defn- put-all [body]
  "=>")

(defcodec beanstalkd-codec
  (header token commands-mapping put-all))