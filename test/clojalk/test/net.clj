(ns clojalk.test.net
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core net utils])
  (:use [lamina.core])
  (:use [clojure.test]))

(deftest test-put
  (let [ch (channel ["PUT" "5" "0" "9" "abc"])
        addr "127.0.0.1:19875"]
    (receive-all ch #(command-dispatcher ch addr %))
    
    (receive-all ch #(is (= (first %) "INSERTED")))))
