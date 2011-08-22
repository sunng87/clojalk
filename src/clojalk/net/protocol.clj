(ns clojalk.net.protocol
  (:use [clojalk.utils])
  (:use [gloss.core])
  (:use [clojure.string :only [lower-case]]))

;; a wrapper for string-integer, copied from ztellman's aleph redis client
;; this codec adds an offset to string length, which is common seen in text
;; based protocol (message with \r\n as suffix)
(defn string-length-and-offset [count-offset]
  (prefix 
    (string-integer :ascii :delimiters ["\r\n"] :as-str true)
    #(if (neg? %) 0 (+ % count-offset))
    #(if-not % -1 (- % count-offset))))

;; --------- gloss codec definitions -----------
(defcodec token (string :ascii :delimiters ["\r\n" " " "\n"]))
(defcodec body 
  (finite-frame
     (string-length-and-offset 2)
     (string :utf8 :suffix "\r\n")))

(def codec-map 
  {;; request headers
   "quit" []
   "list-tubes" []
   "list-tube-used" []
   "list-tubes-watched" []
   "peek" [token]
   "peek-ready" []
   "peek-buried" []
   "peek-delayed" []
   "watch" [token]
   "ignore" [token]
   "use" [token]
   "pause-tube" [token token]
   "reserve" []
   "reserve-with-timeout" [token]
   "release" [token token token]
   "delete" [token]
   "touch" [token]
   "bury" [token token]
   "kick" [token]
   "put" [token token token body]
   
   ;; response headers
   "inserted" [token]
   "reserved" [token body]})

(defn- commands-mapping [cmd]
  (let [normalized-cmd (lower-case (dbg cmd))]
    (if (contains? codec-map normalized-cmd)
      (compile-frame (dbg (codec-map normalized-cmd)) #(rest %) #(cons normalized-cmd %))
      (string :utf8 :delimiters ["\r\n"]))))

(defn- empty-header [body] "")

(defcodec beanstalkd-codec
  (header token commands-mapping first))
