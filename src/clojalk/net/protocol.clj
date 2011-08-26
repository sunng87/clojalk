(ns clojalk.net.protocol
  (:use [clojalk.utils])
  (:use [gloss.core])
  (:use [clojure.string :only [upper-case]]))

;; a wrapper for string-integer, copied from ztellman's aleph redis client
;; this codec adds an offset to string length, which is common seen in text
;; based protocol (message with \r\n as suffix)
(defn string-length-and-offset [count-offset]
  (prefix 
    (string-integer :ascii :delimiters ["\r\n"] :as-str true)
    #(if (neg? %) 0 (+ % count-offset))
    #(if-not % -1 (- % count-offset))))

;; --------- gloss codec definitions -----------
(defcodec token (string :ascii :delimiters [" " "\r\n" "\n"]))
(defcodec token-space (string :ascii :delimiters [" "]))
(defcodec token-newline (string :ascii :delimiters ["\r\n"]))
(defcodec body 
  (finite-frame
     (string-length-and-offset 2)
     (string :utf8 :suffix "\r\n")))

(def codec-map 
  {;; request headers
   "QUIT" []
   "LIST-TUBES" []
   "LIST-TUBE-USED" []
   "LIST-TUBES-WATCHED" []
   "PEEK" [token]
   "PEEK-READY" []
   "PEEK-BURIED" []
   "PEEK-DELAYED" []
   "WATCH" [token]
   "IGNORE" [token]
   "USE" [token]
   "PAUSE-TUBE" [token token]
   "RESERVE" []
   "RESERVE-WITH-TIMEOUT" [token]
   "RELEASE" [token token token]
   "DELETE" [token]
   "TOUCH" [token]
   "BURY" [token token]
   "KICK" [token]
   "PUT" [token token token body]
   "STATS-JOB" [token]
   "STATS-TUBE" [token]
   "STATS" []
   
   ;; response headers
   "INSERTED" [token-newline]
   "RESERVED" [token-space body]
   "USING" [token-newline]
   "WATCHING" [token-newline]
   "BAD_FORMAT" [token-newline]
   "NOT_IGNORED" [token-newline]
   "INTERNAL_ERROR" [token-newline]
   "UNKNOWN_COMMAND" [token-newline]
   "OK" [body]
   "RELEASED" [token-newline]
   "BURIED" [token-newline]
   "NOT_FOUND" [token-newline]
   "DELETED" [token-newline]
   "KICKED" [token-newline]
   "TOUCHED" [token-newline]
   "FOUND" [token-space body]
   "TIMED_OUT" [token-newline]
   "PAUSED" [token-newline]
   "DRAINING" [token-newline]})

(defn- commands-mapping [cmd]
  (let [normalized-cmd (upper-case cmd)]
    (if (contains? codec-map normalized-cmd)
      (compile-frame (codec-map normalized-cmd) 
                     #(if (empty? (rest %)) [""] (rest %)) 
                     #(cons normalized-cmd %))
      (string :utf8 :delimiters ["\r\n"]))))

(defn- empty-header [body] "")

(defn- find-header [resp]
  (if (and (vector? resp) (> (count resp) 1)) (first resp)))

(defcodec beanstalkd-codec
  (header token commands-mapping first))
