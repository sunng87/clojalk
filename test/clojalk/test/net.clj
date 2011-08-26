(ns clojalk.test.net
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core net utils])
  (:use [lamina.core])
  (:use [clojure.test]))

(defmacro with-ch [ch data-in addr-in & body]
  `(let [~ch (channel ~data-in)
         addr# {:remote-addr ~addr-in}]
     (receive-all ~ch #(command-dispatcher ~ch addr# %))     
     ~@body))

(defmacro is-ch [ch test]
  `(receive-all ~ch #(is (~test %))))

(deftest test-put
  (with-ch ch ["PUT" "5" "0" "9" "abc"] "127.0.0.1:19875"
    (is-ch ch #(= "INSERTED" (first %)))))

(deftest test-use
  (with-ch ch ["USE" "tomcat"] "127.0.0.1:19876"
    (is-ch ch #(= ["USING" "tomcat"] %))))

(deftest test-watch
  (with-ch ch ["WATCH" "tomcat"] "127.0.0.1:19877"
    (is-ch ch #(= ["WATCHING" "2"] %))))

(deftest test-ignore
  (with-ch ch ["IGNORE" "default"] "127.0.0.1:19878"
    (is-ch ch #(= ["NOT_IGNORED"] %))))       

(deftest test-peekdelayed
  (with-ch ch ["PEEK-DELAYED"] "127.0.0.1:19879"
    (is-ch ch #(= ["NOT_FOUND"] %)))) 

