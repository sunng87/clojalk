(ns clojalk.test.core
  (:refer-clojure :exclude [use peek])
  (:use [clojalk.core])
  (:use [clojalk.utils])
  (:use [clojure.test]))


(deftest test-put
  (let [session (use (open-session :producer) "test-put")]
    (put session 5 0 1000 "")
    (put session 3 0 1002 "")
    (put session 2 100 1000 "")
    (is (= 2 (count @(:ready_set (:test-put @tubes)))))
    (is (= 1 (count @(:delay_set (:test-put @tubes)))))
    (is (= 3 (-> @(:ready_set (:test-put @tubes))
                 first
                 :priority)))
    (is (= :test-put (-> @(:ready_set (:test-put @tubes))
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
      (is (= 1 (count @(:ready_set (:test @tubes)))))
      (is (empty? @(:waiting_list (:test @tubes)))))

    ;; reserve a job from empty tube
    (let [job (reserve session-e)]
      (is (nil? job))
      (is (not-empty @(:waiting_list (:empty @tubes)))))))

(deftest test-delete
  (let [session-p (use (open-session :producer) "delete-test")
        session-w (watch (open-session :worker) "delete-test")
        ;; make some jobs in the delete-test tube
        j1 (put session-p 3 0 1000 "neat")
        j2 (put session-p 4 0 1000 "nice")
        j3 (put session-p 4 0 1000 "cute")]
    
    ;; reserve and delete a job
    (let [job (reserve session-w)
          detached-job (delete session-w (:id job))]
      (is (= :invalid (:state detached-job)))
      (is (= 3 (:priority detached-job))))
    
    ;; bury and delete a job
    (let [job (bury session-w (:id j3) 10)
          detached-job (delete session-w (:id job))]
      (is (empty? @(:buried_list (:delete-test @tubes)))))
    
    ;; delete a ready job
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
        j1 (put session-p 9 0 100 "neat")
        j2 (put session-p 8 10 100 "nice")]
    (is (= "neat" (:body (peek session-p (:id j1)))))
    (is (nil? (peek session-p (:id j2)))) ;;delayed job cannot be found with peek
    (is (nil? (peek session-p 1001)))))

(deftest test-peek-ready
  (let [session-p (use (open-session :producer) "peek-ready-test")]
    (put session-p 9 0 100 "neat")
    (put session-p 10 0 100 "cute")
    (put session-p 8 10 100 "nice")
    (is (= "neat" (:body (peek-ready session-p))))))

(deftest test-peek-delayed
  (let [session-p (use (open-session :producer) "peek-delayed-test")]
    (put session-p 9 0 100 "neat")
    (put session-p 8 10 100 "nice")
    (put session-p 8 20 100 "cute")
    (is (= "nice" (:body (peek-delayed session-p))))))

(deftest test-bury
  (let [session-p (use (open-session :producer) "bury-test")
        session-w (watch (open-session :worker) "buty-test")
        j0 (put session-p 5 0 100 "nice")]
    
    ;; bury j0
    (bury session-w (:id j0) 10)
    
    (is (= 1 (count @(:buried_list (:bury-test @tubes)))))
    (is (= 10 (:priority (first @(:buried_list (:bury-test @tubes))))))
    (is (= :buried (:state (first @(:buried_list (:bury-test @tubes))))))))

(deftest test-kick
  (let [session-p (use (open-session :producer) "kick-test")
        j0 (put session-p 10 0 100 "neat")
        j1 (put session-p 10 0 100 "nice")]
        
    ;; kick empty
    (kick session-p 100)
    
    (is (= 0 (count @(:buried_list (:kick-test @tubes)))))
    (is (= 2 (count @(:ready_set (:kick-test @tubes)))))
    (is (= 0 (count @(:delay_set (:kick-test @tubes)))))
    
    ;; make some jobs, ready and delayed
    (put session-p 20 0 100 "cute")
    (put session-p 20 20 100 "peak")
    (put session-p 25 20 100 "geek")
    
    ;; bury some jobs
    (bury session-p (:id j0) 10)
    (bury session-p (:id j1) 10)
    
    (is (= 2 (count @(:buried_list (:kick-test @tubes)))))
    (is (= 1 (count @(:ready_set (:kick-test @tubes)))))
    (is (= 2 (count @(:delay_set (:kick-test @tubes)))))
    
    (kick session-p 100)
    
    (is (= 0 (count @(:buried_list (:kick-test @tubes)))))
    (is (= 3 (count @(:ready_set (:kick-test @tubes)))))
    (is (= 2 (count @(:delay_set (:kick-test @tubes)))))
    
    (kick session-p 1)
    
    (is (= 0 (count @(:buried_list (:kick-test @tubes)))))
    (is (= 4 (count @(:ready_set (:kick-test @tubes)))))
    (is (= 1 (count @(:delay_set (:kick-test @tubes)))))
    
    (is (every? #(= :ready (:state %)) @(:ready_set (:kick-test @tubes))))))

(deftest test-touch
  (let [session-p (use (open-session :producer) "touch-test")
        session-w (watch (open-session :worker) "touch-test")
        j0 (put session-p 5 0 100 "nice")
        j0_ (reserve session-w)]
    
    (sleep 0.3)
    (is (> (:deadline_at (touch session-w (:id j0_))) (:deadline_at j0_)))))

(deftest test-update-expired-task
  (let [session-p (use (open-session :producer) "expire-task-test")
        session-w (watch (open-session :worker) "expire-task-test")]
    ;;make some jobs in the tube
    (put session-p 8 0 1 "nice")
    (put session-p 9 0 1 "neat")
    (put session-p 10 0 1 "cute")
    (put session-p 9 0 10 "geek")
    
    (is (= 4 (count @(:ready_set (:expire-task-test @tubes)))))
    
    ;;reserve some jobs from the tube
    (reserve session-w)
    (reserve session-w)
    (reserve session-w)
    
    (is (= 1 (count @(:ready_set (:expire-task-test @tubes)))))
    
    ;;wait for expire
    (sleep 1.5)
    (update-expired-job-task)
    
    (is (= 3 (count @(:ready_set (:expire-task-test @tubes)))))))

(deftest test-update-expired-tube
  (let [session-p (use (open-session :producer) "expire-tube-test")]
    (pause-tube session-p "expire-tube-test" 0.5)
    (is (true? @(:paused (:expire-tube-test @tubes))))
    
    (sleep 0.8)
    (update-paused-tube-task)
    
    (is (false? @(:paused (:expire-tube-test @tubes))))))


(deftest test-pending-reserved-session
  (let [session-p (use (open-session :producer) "pending-test")
        session-w (watch (open-session :worker) "pending-test")]
    ;; waiting for incoming job
    (reserve session-w)
    (is (= :waiting (:state @session-w)))
    
    ;; put a job
    (put session-p 10 0 20 "nice")
    
    (is (= "nice" (:body (:incoming_job @session-w))))
    (is (= :working (:state @session-w)))))
