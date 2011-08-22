(ns clojalk.net.protocol
  (:use [gloss.core])
  (:use [clojure.string :only [trim]]))


;; --------- gloss codec definitions -----------
(defcodec token (string :ascii :delimiters [" " "\n" "\r\n"]))
(defcodec body 
  (compile-frame
    [(finite-frame
       (string-integer :ascii :delimiters ["\n" "\r\n"])
       (string :utf8))
     (string :utf8 :suffix "\r\n")]
    nil first))

(defcodec quit-codec
          (compile-frame [] #() #(cons "quit" %)))

(defcodec reserve-codec
          (compile-frame [token] #() #(cons "reserve" %)))

(defcodec put-codec
          (compile-frame [token token token body]
                         #() #(cons "put" %)))

(defn- commands-mapping [cmd]
  (case cmd
    "quit" quit-codec
    "put" put-codec
    "reserve" reserve-codec
    
    "=>" (string :utf8 :suffix "\r\n")))

(defn- put-all [body]
  "=>")

(defcodec beanstalkd-codec
  (header token commands-mapping put-all))