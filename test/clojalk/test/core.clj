(ns clojalk.test.core
  (:use [clojalk.core])
  (:use [clojalk.utils])
  (:use [clojure.test]))


(deftest test-put
  (let [session (open-session :producer)]
    (put session 5 0 1000 "")
    (put session 3 0 1002 "")
    (put session 2 100 1000 "")       
    (is (= 2 (count @jobs)))
    (is (= 2 (count @(:ready_set (:default @tubes)))))
    (is (= 1 (count @(:delay_set (:default @tubes)))))
    (is (= 3 (-> @(:ready_set (:default @tubes))
                 first
                 :priority)))
    (is (= :default (-> @(:ready_set (:default @tubes))
                 first
                 :tube)))))

(deftest test-reserve
  (let [session-p (use (open-session :producer) "test")
        session-t (watch (open-session :worker) "test")
        session-e (watch (open-session :worker) "empty")]
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
        session-w (assoc (open-session :worker) :watch '(:delete-test))
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
    
(deftest test-release
  (let [session-p (use (open-session :producer) "release-test")
        session-w (watch (open-session :worker) "release-test")
        ;; make some jobs in the release-test cube
        j1 (put session-p 3 0 1000 "neat")
        j2 (put session-p 4 0 1000 "nice")]
    
    ;; reserve a job then release it
    (let [job (reserve session-w)]
      (release session-w (:id job) 5 0)
      (is (= 2 (count @(:ready_set (:release-test @tubes))))))
    
    (let [job (reserve session-w)]
      (release session-w (:id job) 10 100)
      (is (= 1 (count @(:ready_set (:release-test @tubes)))))
      (is (= 5 (-> @(:ready_set (:release-test @tubes)) first :priority))))))

(defn- sleep [seconds]
  (Thread/sleep (* 1000 seconds)))

(deftest test-update-delay-task
  (let [session-p (use (open-session :producer) "delay-task-test")]
    ;; add some delayed job 
    (put session-p 3 1 1000 "neat")
    (put session-p 4 2 1000 "nice")
    (put session-p 5 10 1000 "cute")
    (put session-p 8 0 1000 "")
    
    (is (= 3 (count @(:delay_set (:delay-task-test @tubes)))))
    (is (= 1 (count @(:ready_set (:delay-task-test @tubes)))))
    
    ;; sleep 
    (sleep 3)
    
    ;; update tasks
    (update-delay-job-task)
    
    (is (= 1 (count @(:delay_set (:delay-task-test @tubes)))))
    (is (= 3 (count @(:ready_set (:delay-task-test @tubes)))))))

(deftest test-peek
  (let [session-p (use (open-session :producer) "peek-test")
        session-w (watch (open-session :worker) "peek-test")
        j1 (put session-p 9 0 100 "neat")
        j2 (put session-p 8 10 100 "nice")]
    (is (= "neat" (:body (peek session-w (:id j1)))))
    (is (nil? (peek session-w (:id j2)))) ;;delayed job cannot be found with peek
    (is (nil? (peek session-w 1001)))))

(deftest test-peek-ready
   (let [session-p (use (open-session :producer) "peek-ready-test")
         session-w (watch (open-session :worker) "peek-ready-test")]
     (put session-p 9 0 100 "neat")
     (put session-p 10 0 100 "cute")
     (put session-p 8 10 100 "nice")
     (is (= "neat" (:body (peek-ready session-w))))))

(deftest test-peek-delay
   (let [session-p (use (open-session :producer) "peek-delay-test")
         session-w (watch (open-session :worker) "peek-delay-test")]
     (put session-p 9 0 100 "neat")
     (put session-p 8 10 100 "nice")
     (put session-p 8 20 100 "cute")
     (is (= "nice" (:body (peek-delay session-w))))))
