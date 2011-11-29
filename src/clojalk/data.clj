;; ## Stateful containers hold data at runtime
;;
(ns clojalk.data
  (:use [clojalk.utils]))

;; # Data Structures and constructors

;; Structure definition for ***Job***
;;
;; **Job** is the basic task unit in clojalk. The fields are described below.
;;
;; * **id** a numerical unique id of this Job
;; * **delay** delayed time in seconds.
;; * **ttr** time-to-run in seconds. TTR is the max time that a worker could reserve this job.
;; The job will be released once it's timeout.
;; * **priority** describes the priority of jobs. The value should be in range of 0-65535.
;; Job with lower numerical value has higher priority.
;; * **created_at** is the timestamp when job was created, in milliseconds.
;; * **deadline_at** is to stored the deadline of a job, in milliseconds. The fields has
;; multiple meaning according to the *state*. In a word, it's the time that job should update
;; its state.
;; * **state** is a keyword enumeration. It's the most important field that describes
;; the life-cycle of a Job.
;;   1. **:ready** the job is ready for worker to consume.
;;   1. **:delayed** the job is not ready until the deadline hit.
;;   1. **:reserved** indicates the job is reserved by a worker at that time.
;;   1. **:buried** indicatets the job could not be reserved until someone ***kick***s it.
;;   1. **:invalid** means the job has been deleted.
;; * **tube** is the keyword tube name of this job
;; * **body** the body of this job
;; * **reserver** the session holds this job. nil if the job is not reserved.
;; * **reserves**, **timeouts**, **releases**, **buries** and **kicks** are statistical field
;; to indicate how many times the job reserved, timeout, released, buried and kicked.
;;
(defstruct Job :id :delay :ttr :priority :created_at
  :deadline_at :state :tube :body :reserver
  :reserves :timeouts :releases :buries :kicks)

;; Structure definition for Tube
;;
;; Tube is a collection of jobs, similar to the database in RDBMS.
;;
;; * **name** the name of this tube, as keyword.
;; * **ready_set** is a sorted set of jobs in ready state. Jobs are sorted with their priority.
;; * **delay_set** is a sorted set of jobs in delayed state. Jobs are sorted with their deadline.
;; * **buried_list** is a vector of buried jobs.
;; * **waiting_list** is a vector of pending workers.
;; * **paused** indicates whether the tube has been paused or not.
;; * **pause_deadline** is the time to end the pause state.
;; * **pauses** is a statistical field of how many times the tube paused.
;;
(defstruct Tube :name :ready_set :delay_set :buried_list
  :waiting_list :paused :pause_deadline :pauses)

;; Structure definition for Session (connection in beanstalkd)
;;
;; Session represents all clients connected to clojalk.
;;
;; * **id** the id of this session
;; * **type** is a keyword enumeration indicates the role of a session. (worker or producer)
;; * **use** the tube name that producer session is using
;; * **watch** a list of tube names that worker session is watching
;; * **deadline_at** is the timeout for reserve request of worker session
;; * **state** of a worker session:
;;   1. **:idle** the worker session is idle
;;   1. **:waiting** the worker session has sent reserve request, is now waiting for jobs
;;   1. **:working** the worker session has reserved a job
;; * **incoming_job** the job worker session just reserved
;; * **reserved_jobs** id of jobs the worker session reserved
;;
(defstruct Session :id :type :use :watch :deadline_at :state
  :incoming_job :reserved_jobs)

;; A generic comparator for job:
;;  Compare selected field or id if equal.
(defn- job-comparator [field j1 j2]
  (cond
    (< (field j1) (field j2)) -1
    (> (field j1) (field j2)) 1
    :else (< (:id j1) (:id j2))))

;; Curried job-comparator by *priority*
(def priority-comparator
  (partial job-comparator :priority))

;; Curried job-comparator by *delay*
(def delay-comparator
  (partial job-comparator :delay))

;; Function to create an empty tube.
(defn make-tube [name]
  (struct Tube (keyword name) ; name
          (ref (sorted-set-by priority-comparator)) ; ready_set
          (ref (sorted-set-by delay-comparator)) ; delay_set
          (ref []) ; buried_list
          (ref []) ; waiting queue
          (ref false) ; paused state
          (ref -1) ; pause timeout
          (ref 0))) ; pause command counter

;; Default job id generator. We use an atomic integer to store id.
(defonce id-counter (atom (long 0)))
;; Get next id by increase the id-counter
(defn next-id []
  (swap! id-counter inc))

;; Function to create an empty job with given data.
(defn make-job [priority delay ttr tube body]
  (let [id (next-id)
        now (current-time)
        created_at now
        deadline_at (+ now (* 1000 delay))
        state (if (> delay 0) :delayed :ready)]
    (struct Job id delay ttr priority created_at
            deadline_at state tube body nil
            0 0 0 0 0)))

;;
;; Field to indicate if the server is in a drain mode.
;; If the server is drained, it doesn't accept new job any more.
(defonce drain (atom false))

;; Function to toggle drain mode.
(defn toggle-drain []
  (swap! drain not))

;; **jobs** is a referenced hash map holds all jobs with id as key.
(defonce jobs (ref {}))
;; **tubes** is a referenced hash map for all tubes, with their name as key
(defonce tubes (ref {:default (make-tube "default")}))
;; **commands** is for command stats. commands are assigned into this map when it's defined
(defonce commands (ref {}))
;; start time
(defonce start-at (current-time))

;; All **sessions** are stored in this referenced map. id as key.
(defonce sessions (ref {}))
;; A statistical field for job timeout count.
;; Note that we use a ref here because timeout check of jobs are inside a dosync block which
;; should be free of side-effort. If we use an atom here, it could be error in retry.
(defonce job-timeouts (atom 0))
