(ns clojalk.test.core
  (:use [clojalk.core])
  (:use [clojure.test]))


(deftest test-put
  (let [session (open-session :producer)]
    (put session 5 0 1000 "")
    (put session 3 0 1002 "")       
    (is (= 2 (count @jobs)))
    (is (= 2 (count @(:ready_set (:default @cubes)))))
    (is (= 3 (-> @(:ready_set (:default @cubes))
                 first
                 :jobspec
                 :priority)))))

