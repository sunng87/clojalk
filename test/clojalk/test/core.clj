(ns clojalk.test.core
  (:use [clojalk.core])
  (:use [clojalk.utils])
  (:use [clojure.test]))


(deftest test-put
  (let [session (open-session :producer)]
    (put session 5 0 1000 "")
    (put session 3 0 1002 "")       
    (is (= 2 (count @jobs)))
    (is (= 2 (count @(:ready_set (:default @tubes)))))
    (is (= 0 (count @(:delay_set (:default @tubes)))))
    (is (= 3 (-> @(:ready_set (:default @tubes))
                 first
                 :priority)))
    (is (= :default (-> @(:ready_set (:default @tubes))
                 first
                 :tube)))))

(deftest test-reserve
  (let [session-p (use (open-session :producer) "test")
        session-t (assoc (open-session :worker) :watches '(:test))
        session-e (assoc (open-session :worker) :watches '(:empty))]
    ;; make some jobs in test tube
    (put session-p 3 0 1000 "")
    (put session-p 10 0 100 "")
    
    ;; reserve a job from test tube
    (let [job (reserve session-t)]
      (is (not (nil? job)))
      (is (= 3 (:priority job)))
      (is (= :reserved (:state job)))
      (is (= 1 (count @(:ready_set (:test @tubes))))))

    ;; reserve a job from empty tube
    (let [job (reserve session-e)]
      (is (nil? job)))))

(deftest test-delete
  (let [session-p (use (open-session :producer) "delete-test")
        session-w (assoc (open-session :worker) :watches '(:delete-test))
        ;; make some jobs in the delete-test tube
        j1 (put session-p 3 0 1000 "neat")
        j2 (put session-p 4 0 1000 "nice")]
    
    ;; reserve and delete a job
    (let [job (reserve session-w)
          detached-job (delete session-w (:id job))]
      (is (= :invalid (:state detached-job)))
      (is (= 3 (:priority detached-job))))
    
    ;; delete 
    (delete session-w (:id j2))
    
    ;; make sure tube is empty
    (is (empty? @(:ready_set (:delete-test @tubes))))))
    
    
    