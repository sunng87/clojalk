(ns clojalk.test.net
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core net utils])
  (:use [lamina.core])
  (:use [clojure.test]))

(defmacro is-ch [ch test]
  `(receive-all ~ch #(is (~test %))))

(deftest test-put
  (let [ch (channel ["PUT" "5" "0" "9" "abc"])
        addr {:remote-addr "127.0.0.1:19875"}]
    (receive-all ch #(command-dispatcher ch addr %))
    
    (is-ch ch #(= "INSERTED" (first %)))))

(deftest test-use
  (let [ch (channel ["USE" "tomcat"])
        addr {:remote-addr "127.0.0.1:19876"}]
    (receive-all ch #(command-dispatcher ch addr %))
    
    (is-ch ch #(= ["USING" "tomcat"] %))))

(deftest test-watch
  (let [ch (channel ["WATCH" "tomcat"])
        addr {:remote-addr "127.0.0.1:19877"}]
    (receive-all ch #(command-dispatcher ch addr %))
    
    (is-ch ch #(= ["WATCHING" "2"] %))))

(deftest test-ignore
  (let [ch (channel ["IGNORE" "default"])
        addr {:remote-addr "127.0.0.1:19878"}]
    (receive-all ch #(command-dispatcher ch addr %))
    
    (is-ch ch #(= ["NOT_IGNORED"] %))))
