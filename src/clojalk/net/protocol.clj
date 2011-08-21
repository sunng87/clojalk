(ns clojalk.net.protocol
  (:use [gloss.core])
  (:use [clojure.string :only [trim]]))


;; --------- gloss codec definitions -----------
(defcodec token (string :utf-8 :delimiters [" " "\n" "\r\n"]))
(defcodec body 
  (compile-frame
    [(finite-frame
       (string-integer :ascii :delimiters ["\n" "\r\n"])
       (string :utf8))
     (string :utf8 :delimiters ["\n" "\r\n"])] nil first))

(defcodec quit-codec
          (compile-frame [] #() #(cons "quit" %)))

(defcodec reserve-codec
          (compile-frame [token] #() #(cons "reserve" %)))

(defcodec put-codec
          (compile-frame [token token token body]
                         #() #(cons "put" %)))

(defn- commands-mapping [cmd]
  (case (trim cmd)
    "quit" quit-codec
    "put" put-codec
    "reserve" reserve-codec
    nil))

(defn- put-all [body]
  string)

(defcodec beanstalkd-codec
  (header token commands-mapping put-all))