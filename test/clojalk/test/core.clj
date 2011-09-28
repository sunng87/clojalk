(ns clojalk.test.core
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core utils data])
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
      (is (contains? (:reserved_jobs @session-t) (:id job)))
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
        j2 (put session-p 10 0 1000 "nice")
        j3 (put session-p 4 0 1000 "cute")
        j4 (put session-p 100 100 1000 "geek")]
    
    ;; reserve and delete a job
    (let [job (reserve session-w)
          detached-job (delete session-w (:id job))]
      (is (= :invalid (:state detached-job)))
      (is (= 3 (:priority detached-job))))
    
    ;; bury and delete a job
    (let [job (reserve session-w)
          job (bury session-w (:id job) 10)
          detached-job (delete session-w (:id job))]
      (is (empty? @(:buried_list (:delete-test @tubes))))
      (is (nil? (@jobs (:id job)))))
    
    ;; delete a ready job with non-worker session
    (delete session-p (:id j2))
    (is (empty? @(:ready_set (:delete-test @tubes))))
    
    ;; delayed job could not be deleted
    (is (nil? (delete session-p (:id j4))))))
    
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
    
    (is (= 1 (count @(:delay_set (:delay-task-test @tubes)))))
    (is (= 3 (count @(:ready_set (:delay-task-test @tubes)))))))

(deftest test-peek
  (let [session-p (use (open-session :producer) "peek-test")
        j1 (put session-p 9 0 100 "neat")
        j2 (put session-p 8 10 100 "nice")]
    (is (= "neat" (:body (peek session-p (:id j1)))))
    (is (peek session-p (:id j2))) ;;delayed job can also be found with peek
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
        session-w (watch (open-session :worker) "bury-test")
        j0 (put session-p 5 0 100 "nice")]
    
    ;; bury j0
    (reserve session-w)
    (bury session-w (:id j0) 10)
    
    (is (= 1 (count @(:buried_list (:bury-test @tubes)))))
    (is (= 10 (:priority (first @(:buried_list (:bury-test @tubes))))))
    (is (= :buried (:state (first @(:buried_list (:bury-test @tubes))))))))

(deftest test-kick
  (let [session-p (use (open-session :producer) "kick-test")
        session-w (watch (open-session :worker) "kick-test")
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
    (reserve session-w)
    (reserve session-w)
    
    (bury session-w (:id j0) 10)
    (bury session-w (:id j1) 10)
    
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
    (sleep 1.1)
    (is (= 3 (count @(:ready_set (:expire-task-test @tubes)))))
    (is (= 1 (count (:reserved_jobs @session-w))))))

(deftest test-update-expired-tube
  (let [session-p (use (open-session :producer) "expire-tube-test")
        session-w (watch (open-session :worker) "expire-tube-test")]
    (put session-p 100 0 500 "nice")
    (pause-tube session-p "expire-tube-test" 0.5)
    (is (true? @(:paused (:expire-tube-test @tubes))))
    
    ;; working should be waiting for tube to continue
    (reserve session-w)
    (is (= :waiting (:state @session-w)))
    
    ;; job could be automatically assign to pending worker
    (is (= :working (:state @session-w)))
    (is (false? @(:paused (:expire-tube-test @tubes))))))


(deftest test-pending-reserved-session
  (let [session-p (use (open-session :producer) "pending-test")
        session-w (watch (open-session :worker) "pending-test")
        session-w2 (watch (open-session :worker) "pending-test")]
    ;; waiting for incoming job
    (reserve session-w)
    (is (= :waiting (:state @session-w)))
    (reserve session-w2)
    
    ;; put a job
    (put session-p 10 0 20 "nice")
    
    (is (= "nice" (:body (:incoming_job @session-w))))
    (is (= :working (:state @session-w)))
    
    (let [the-job-id (:id (:incoming_job @session-w))]
      ;; release it
      (release session-w the-job-id 10 0)
      
      ;; it should be reserved by session-w2 immediately
      (is (= :working (:state @session-w2)))
      (is (= :reserved (:state (get @jobs the-job-id))))
      (is (= session-w2 (:reserver (get @jobs the-job-id))))
      (is (empty? @(:waiting_list (:pending-test @tubes)))))
    
    ;; reserve and acquire the job
    (reserve session-w)
    (put session-p 10 0 20 "neat")
    (is (= :working (:state @session-w)))
    
    ;; bury it
    (let [the-job-id (:id (:incoming_job @session-w))]
      (bury session-w the-job-id 10)
      (is (= :idle (:state @session-w)))
      (is (= 1 (count @(:buried_list (:pending-test @tubes)))))
      
      (reserve session-w)
      
      ;; kick it to ready
      (kick session-p 10)
      (is (= :reserved (:state (get @jobs the-job-id))))
      (is (= :working (:state @session-w))))))

(deftest test-reserve-timeout
  (let [session-w (watch (open-session :worker) "test-reserve-timeout")]
    ;;reserve an empty tube with timeout
    (reserve-with-timeout session-w 0.5)
    (is (= 1 (count @(:waiting_list (:test-reserve-timeout @tubes)))))
    
    (is (empty? @(:waiting_list (:test-reserve-timeout @tubes))))
    (is (= :idle (:state @session-w)))))

(deftest test-stats-tube
  (let [tube-name "test-stats-tube"
        session-p (use (open-session :producer) tube-name)
        session-w (watch (open-session :worker) tube-name)]
    ;; put some jobs
    (put session-p 2000 0 5000 "nice")
    (put session-p 2500 0 5000 "neat")
    (put session-p 2500 10 5000 "loop")
    (put session-p 1000 0 5000 "geek")
    (put session-p 999 0 400 "joke")
    
    (reserve session-w)
    (let [stats (stats-tube nil tube-name)]
      (is (= (name (:name stats)) tube-name))
      (is (= (:current-jobs-urgent stats) 1))
      (is (= (:current-jobs-delayed stats) 1))
      (is (= (:current-jobs-reserved stats) 1))
      (is (= (:total-jobs stats) 5))
      (is (false? (:pause stats)))
      (is (zero? (:pause-time-left stats))))))

(deftest test-drain
  (toggle-drain)
  (let [session-p (use (open-session :producer) "test-drain")]
    (is (true? @drain))
    (is (nil? (put session-p 5 0 100 "nice"))))
  (toggle-drain))
