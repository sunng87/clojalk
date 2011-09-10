;; # The core part of clojalk
;;
;; This is the core logic and components of clojalk. It is designed to be used
;; as a embed library or standalone server. So the APIs here are straight forward
;; enough as the server exposed.
;;
;; There are several models in clojalk.
;;
;; * **Session** represents a client (or client thread in embedded usage) that connected
;; to clojalk. Session could be either a ***worker*** or a ***producer***. A producer puts
;; jobs into clojalk. A worker consumes jobs and do predefined tasks describe by job body.
;; * **Tube** is an isolate collection of jobs. A producer session should select a tube to
;; ***use*** before it puts jobs into clojalk. And a worker session could ***watch*** several
;; tubes and consume jobs associated with them. By default, a new producer/worker is
;; using/watching the ***default*** tube. Tube could be created when you start to using and
;; watching it, so there is no such *create-tube* command.
;; * **Job** is the basic task unit in clojalk. A job contains some meta information and
;; a text body that you can put your task description in. I will explain fields of job later.
;;
(ns clojalk.core
  (:refer-clojure :exclude [use peek])
  (:use [clojalk.utils]))

;; ## Data Structures and constructors

;; Structure definition for ***Job***
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
;; Tube is a collection of jobs, similar to the database in RDBMS.
;;
;; * **name** the name of this tube, as keyword.
;; * **ready_set** is a sorted set of jobs in ready state. Jobs are sorted with their priority.
;; * **delay_set** is a sorted set of jobs in delayed state. Jobs are sorted with their deadline.
;; * **buried_list** is a vector of buried jobs.
;; * **waiting_list** is a vector of ids of pending workers.
;; * **paused** indicates whether the tube has been paused or not.
;; * **pause_deadline** is the time to end the pause state.
;; * **pauses** is a statistical field of how many times the tube paused.
;;
(defstruct Tube :name :ready_set :delay_set :buried_list 
  :waiting_list :paused :pause_deadline :pauses)

;; Structure definition for Session (connection in beanstalkd)
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
;; * **reserved_jobs** id of jobs the worker session reserved
;;
(defstruct Session :id :type :use :watch :state :deadline_at :reserved_jobs)

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
  (ref (struct Tube (keyword name) ; name
          (sorted-set-by priority-comparator) ; ready_set
          (sorted-set-by delay-comparator) ; delay_set
          [] ; buried_list
          [] ; waiting queue
          false ; paused state
          -1 ; pause timeout
          0))) ; pause command counter

;; Default job id generator. We use an atomic integer to store id.
(defonce id-counter (atom 0))
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


;; ## Stateful containers hold data at runtime

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
(defonce job-timeouts (ref 0))

;; ## Functions to handle clojalk logic

;; Find top priority job from session's watch list.
;; We will filter paused tubes in this function, a paused tube will be treated
;; as an empty tube here.
;; Steps:
;;
;; 1. Get watched tube name list
;; 1. Select tubes from @tubes
;; 1. Filter selected tubes to exclude paused tubes
;; 1. Find top priority job from each tube
;; 1. Find top priority job among jobs selected from last step
;;
;; (This function does not open transaction so it should run within a dosync block)
(defn- top-ready-job [session]
  (let [watchlist (:watch @session)
        watch-tubes (filter not-nil (map #(get @tubes %) watchlist))
        watch-tubes (filter #(false? (:paused @%)) watch-tubes)
        top-jobs (filter not-nil (map #(first (:ready_set @%)) watch-tubes))]
    (first (apply sorted-set-by (conj top-jobs priority-comparator)))))

;; Append a session into waiting_list of all tubes it watches.
;; Also update *state* and *deadline_at* of the session.
;;
;; A promise is bound to session to block reserve task
;; (This function does not open transaction so it should run within a dosync block)
(defn- enqueue-waiting-session [session timeout]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name @%)) (vals @tubes))
        deadline_at (if (nil? timeout) nil (+ (current-time) (* timeout 1000)))
        result (agent (promise))]
    (doseq [tube watch-tubes]
      (alter tube assoc :waiting_list (conj (:waiting_list @tube) (:id @session))))
    (alter session assoc
               :state :waiting
               :deadline_at deadline_at
               :waiting_promise result)))

;; Remove session from waiting_list of all tubes it watches.
;; This function is invoked when a session successfully reserved a job.
;; This also updates session *state* to `working` and leave *deadline_at* as it is.
;;
;; (This function does not open transaction so it should run within a dosync block)
(defn- dequeue-waiting-session [session state]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name @%)) (vals @tubes))]
    (doseq [tube watch-tubes]
      (alter tube assoc :waiting_list (into [] (remove-item (:waiting_list @tube) (:id @session)))))
    (alter session assoc :state state
                         :deadline_at nil
                         :waiting_promise nil)))

;; Reserve the job with the session.
;; For timeout session, a nil will be passed into the form.
;; Steps:
;;
;; 1. Run `dequeue-waiting-session` on this session, remove the session from
;; waiting queue. if the job is nil, we will set the state to :idle
;; 1. If the job passed in is a nil, do nothing.
;; 1. Find tube of this job
;; 1. Compute deadline of this reservation
;; 1. Create an updated version of job
;;   - set state to `reserved`
;;   - set reserver to this session
;;   - set deadline_at to deadline of last step
;;   - increase reserve count
;; 1. Remove ths job from its tube's ready_set
;; 1. Update job in @jobs
;; 1. Append the job id to `reserved_jobs` of the session
;;
;; Finally, this function returns the reserved job.
;;
(defn- reserve-job [session job]
  (dosync
    (dequeue-waiting-session session (if (nil? job) :idle :working)))
  (if-not (nil? job)
    (let [tube ((:tube job) @tubes)
          deadline (+ (current-time) (* (:ttr job) 1000))
          updated-top-job (assoc job
                                 :state :reserved
                                 :reserver session
                                 :deadline_at deadline
                                 :reserves (inc (:reserves job)))]
      (dosync
        (alter tube assoc :ready_set (disj (:ready_set @tube) job))
        (alter jobs assoc (:id job) updated-top-job)
        (alter session assoc :reserved_jobs
               (conj (:reserved_jobs @session) (:id updated-top-job)))
        updated-top-job))))

;; We use an agent to deliver job to promise, to prevent STM retry
(defn- notify-waiting-session [session job]
  (send (:waiting_promise @session) deliver job))

;; Mark the job as ready. This is referenced when
;;
;; 1. reserved/delayed job expired
;; 1. reserved job released
;; 1. buried job kicked
;;
;; Steps:
;;
;; 1. Set job state to `ready` and update it in `jobs`
;; 1. Add this job to its tube's ready_set
;; 1. Check if there is any waiting session on that tube, assign the job to it if true
;;
;; (This function does not open transaction so it should run within a dosync block)
(defn- set-job-as-ready [job]
  (let [tube ((:tube job) @tubes)]
    (do
      (alter jobs assoc (:id job) (assoc job :state :ready))
      (alter tube assoc :ready_set (conj (:ready_set @tube) job))
      (if-let [s (@sessions (first (:waiting_list @tube)))]
        (notify-waiting-session s job)))))

;; Create a session and add it to the `sessions`
;; There are two signatures for this function. If you do not provide id, a uuid will be
;; generated as session id.
;; Additional key-value pair (session-data) could also be bound to session.
;;
;; By default, the session will use and watch `default` tube.
;;
(defn open-session 
  ([type] (open-session (uuid) type))
  ([id type & sesssion-data] 
    (let [session (ref (struct Session id type :default #{:default} :idle nil #{}))]
      (dosync
        (if (not-empty sesssion-data)
          (alter session assoc-all sesssion-data))
        (alter sessions assoc id session))
      session)))

;; Close a session with its id
;;
;; Note that we will release all reserved jobs before closing the session.
;; So there won't be any jobs reserved by a dead session.
(defn close-session [id]
  (let [session (@sessions id)]
    (dosync
      (doall (map #(set-job-as-ready (@jobs %)) (:reserved_jobs @session)))
      (alter sessions dissoc id))))

;; ## Macros for convenience of creating and executing commands

;; Define a clojalk command. Besides defining a normal clojure form,
;; this form also add a `cmd-name` entry to `commands` for statistic.
;;
(defmacro defcommand [name args & body]
  (dosync (alter commands assoc (keyword (str "cmd-" name)) (atom 0)))
  `(defn ~(symbol name) ~args ~@body))

;; Execute a command with name and arguments.
;; Also update statistical data.
(defmacro exec-cmd [cmd & args]
  `(do
     (if-let [cnt# (get @commands (keyword (str "cmd-" ~cmd)))]
       (swap! cnt# inc))
     (~(symbol cmd) ~@args)))

;; ## Commands Definitions

;; `put` is a producer task. It will create a new job according to information passed in.
;; When server is in drain mode, it does not store the job and return nil.
;; If dealy is not zero, the job will be created as a delayed job. Delayed
;; job could not be reserved until it's timeout and ready.
(defcommand "put" [session priority delay ttr body]
  (if-not @drain
    (let [tube ((:use @session) @tubes)
          job (make-job priority delay ttr (:name @tube) body)]
      (do
        (dosync
          (case (:state job)
            :delayed (do 
                       (alter tube assoc :delay_set (conj (:delay_set @tube) job))
                       (alter jobs assoc (:id job) job))
            :ready (set-job-as-ready job)))
        job))))

;; `peek` will try to find job with given id. Any session could use this
;; command.
(defcommand "peek" [session id]
  (get @jobs id))

;; `peek-ready` is a producer task. It will peek the most prioritized job from current
;; using tube.
(defcommand "peek-ready" [session]
  (let [tube ((:use @session) @tubes)]
    (first (:ready_set @tube))))

;; `peek-delayed` is also a producer task. The job which is nearest to deadline will
;; be peeked.
(defcommand "peek-delayed" [session]
  (let [tube ((:use @session) @tubes)]
    (first (:delay_set @tube))))

;; `peek-buried` is another producer task. It will peek the first item in the buried list.
(defcommand "peek-buried" [session]
  (let [tube ((:use @session) @tubes)]
    (first (:buried_list @tube))))

;; `reserve-with-timeout` is a worker task. It tries to reserve a job from its watching
;; tubes. If there is no job ready for reservation, it will be blocked at most `timeout`
;; seconds.
;;
;; A promise is set into the session in `enqueue-waiting-session`. When a job is ready
;; for waiting session, it will be delivered to the promise.
(defcommand "reserve-with-timeout" [session timeout]
  (dosync
    (enqueue-waiting-session session timeout))
  (if-let [top-job (top-ready-job session)]
    (notify-waiting-session session top-job))
  (reserve-job session (deref @(:waiting_promise @session))))

;; `reserve` is a worker task. It will wait for available jobs without timeout.
(defcommand "reserve" [session]
  (reserve-with-timeout session nil))

;; `use` is a producer task. It will create a tube if not exist.
(defcommand "use" [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if-not (contains? @tubes tube-name-kw)
        (alter tubes assoc tube-name-kw (make-tube tube-name)))
        (alter session assoc :use tube-name-kw)
      session)))

;; `delete` could be used either with worker or producer. The rule is:
;;
;; 1. For reserved job, only reserved session could delete it
;; so we'd like to reject job that is reserved and its reserver
;; is not current session
;; 2. Delayed job could not be deleted until it's ready
;;
;; Steps to delete a job is a little bit complex:
;; 1. Test if job could satisfy rules described above.
;; 2. Remove job from *jobs*
;; 3. If the job is buried, update `buried_list` of its tube
;; 4. If the job is in ready_set, remove it from ready_set
;; its reserved_jobs list
;; 6. Set the session as idle if the no other jobs reserved by it
;; 7. Set the job as invalid and return
;;
(defcommand "delete" [session id]
  (if-let [job (get @jobs id)]
    (if-not (or (= :delayed (:state job)) 
                (and (= :reserved (:state job)) 
                     (not (= (:id @session) (:id @(:reserver job))))))
      (let [tube ((:tube job) @tubes)]
        (do
          (dosync
            (alter jobs dissoc id)
            (if (= (:state job) :buried)
              (alter tube assoc :buried_list 
                     (vec (remove-item (:buried_list @tube) job))))
            (if (= (:state job) :ready)
              (alter tube assoc :ready_set
                     (disj (:ready_set @tube) job)))
            (alter session assoc :reserved_jobs 
                   (disj (:reserved_jobs @session) (:id job)))
            (if (empty? (:reserved_jobs @session))
              (alter session assoc :state :idle)))
          (assoc job :state :invalid))))))

;; `release` is a worker command to free reserved job and changes its
;; priority and delay. `release` will also check the state and reserver of
;; given job because only reserved job could be released by its reserver.
;;
;; After job released (set-job-as-ready), it will also update session
;; like what we do in `delete`.
(defcommand "release" [session id priority delay]
  (if-let [job (get @jobs id)]
    (if (and (= (:state job) :reserved) 
             (= (:id @(:reserver job)) (:id @session)))
      (let [tube ((:tube job) @tubes)
            now (current-time)
            deadline (+ now (* 1000 delay))
            updated-job (assoc job :priority priority 
                               :delay delay 
                               :deadline_at deadline
                               :releases (inc (:releases job)))]
        (do
          (dosync
            (if (> delay 0)
              (do
                (alter tube assoc :delay_set 
                       (conj (:delay_set @tube) (assoc updated-job :state :delayed))))
              (set-job-as-ready (assoc updated-job :state :ready)))
            (alter session assoc :reserved_jobs 
                   (disj (:reserved_jobs @session) (:id updated-job)))
            (if (empty? (:reserved_jobs @session))
              (alter session assoc :state :idle)))
          updated-job)))))

;; `bury` is a worker task. And only reserved job could be buried by
;; its reserver.
;;
;; buried job will be added into the buried_list of its tube.
(defcommand "bury" [session id priority]
  (if-let [job (get @jobs id)]
    (if (and (= (:state job) :reserved) 
             (= (:id @(:reserver job)) (:id @session)))
      (let [tube ((:tube job) @tubes)
            updated-job (assoc job :state :buried 
                               :priority priority
                               :buries (inc (:buries job)))]
        (do
          (dosync
            (alter tube assoc :buried_list (conj (:buried_list @tube) updated-job))
            (alter jobs assoc (:id updated-job) updated-job)
            (alter session assoc :reserved_jobs 
                   (disj (:reserved_jobs @session) (:id updated-job)))
            (if (empty? (:reserved_jobs @session))
              (alter session assoc :state :idle)))
          updated-job)))))

;; `kick` is a producer command. It will kick at most `bound` jobs from buried
;; or delayed to ready. Buried jobs will be kicked first, if there is no jobs
;; in buried_list, delayed jobs will be kicked. However, it won't kick both set
;; of jobs at a kick. That means, if you have buried jobs less that `bound`, only
;; the buried jobs could be kicked. Delayed ones could be kicked in next `kick`.
(defcommand "kick" [session bound]
  (let [tube ((:use @session) @tubes)]
    (dosync
      (if (empty? (:buried_list @tube))
        ;; no jobs buried, kick from delay set
        (let [kicked (take bound (:delay_set @tube))
              updated-kicked (map #(assoc % :state :ready) kicked)
              remained (drop bound (:delay_set @tube))
              remained-set (apply sorted-set-by delay-comparator remained)]
          
          (alter tube assoc :delay_set remained-set)
          (doseq [job updated-kicked] (set-job-as-ready job))
          updated-kicked)
        
        ;; kick at most bound jobs from buried list
        (let [kicked (take bound (:buried_list @tube))
              updated-kicked (map #(assoc % :state :ready) kicked)
              remained (vec (drop bound (:buried_list @tube)))]
          (alter tube assoc :buried_list remained)
          (doseq [job updated-kicked] 
            (set-job-as-ready (assoc job :kicks (inc (:kicks job)))))
          updated-kicked)))))

;; `touch` is another worker command to renew the deadline. It will perform
;; the same check as `release` does.
;;
(defcommand "touch" [session id]
  (if-let [job (get @jobs id)]
    (if (and (= (:state job) :reserved) 
             (= (:id @(:reserver job)) (:id @session)))
      (let [deadline (+ (current-time) (* (:ttr job) 1000))
            updated-job (assoc job :deadline_at deadline)]
        (dosync
          (if (= :reserved (:state updated-job)) ;; only reserved jobs could be touched
            (do
              (alter jobs assoc (:id updated-job) updated-job)
              updated-job)))))))

;; `watch` is a worker command to add tube into watching list.
;; Will create tube if it doesn't exist.
(defcommand "watch" [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if-not (contains? @tubes tube-name-kw)
        (alter tubes assoc tube-name-kw (make-tube tube-name)))
        (alter session assoc :watch (conj (:watch @session) tube-name-kw))
      session)))

;; `ignore` is a worker command to remove tube from watching list.
;; Note that a worker could not remove the last tube it watches.
(defcommand "ignore" [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if (> (count (:watch @session)) 1)
        (alter session assoc :watch (disj (:watch @session) tube-name-kw))))
    session))

;; stats command. list tubes names.
(defcommand "list-tubes" [session]
  (keys @tubes))

;; stats command. display tube used by current session.
(defcommand "list-tube-used" [session]
  (:use @session))

;; stats command. list tubes watched by current session.
(defcommand "list-tubes-watched" [session]
  (:watch @session))

;; Pause select tube in next `timeout` seconds. Jobs in paused tubes could
;; not be reserved until pause timeout.
;; Also update a statistical field.
(defcommand "pause-tube" [session id timeout]
  (if-let [tube ((keyword id) @tubes)]
    (dosync
      (alter tube assoc :paused true)
      (alter tube assoc :pause_deadline (+ (* timeout 1000) (current-time)))
      (alter tube assoc :pauses (inc (:pauses @tube))))))

;; stats command. Display some information of a job.
(defcommand "stats-job" [session id]
  (if-let [job (get @jobs id)]
    (let [state (:state job)
          now (current-time)
          age (int (/ (- now (:created_at job)) 1000))
          time-left (if (contains? #{:delayed :reserved} state) 
                      (int (/ (- (:deadline_at job) now) 1000)) 0)]
      {:id (:id job)
       :tube (:tube job)
       :state state
       :pri (:priority job)
       :age age
       :delay (:delay job)
       :ttr (:ttr job)
       :reserves (:reserves job)
       :timeouts (:timeouts job)
       :releases (:releases job)
       :buries (:buries job)
       :kicks (:kicks job)
       :time-left time-left})))

;; stats command. Display some information of a tube.
(defcommand "stats-tube" [session name]
  (if-let [tube (get @tubes (keyword name))]
    (let [paused (:paused @tube)
          now (current-time)
          pause-time-left (int (/ (- (:pause_deadline @tube) now) 1000))
          pause-time-left (if paused pause-time-left 0)
          jobs-func #(= (:tube %) (:name @tube))
          jobs-of-tube (filter jobs-func (vals @jobs))
          jobs-reserved (filter #(= (:state %) :reserved) jobs-of-tube)
          jobs-urgent (filter #(< (:priority %) 1024) (:ready_set @tube))]
      {:name (:name @tube)
       :current-jobs-urgent (count jobs-urgent)
       :current-jobs-ready (count (:ready_set @tube))
       :current-jobs-delayed (count (:delay_set @tube))
       :current-jobs-buried (count (:buried_list @tube))
       :current-jobs-reserved (count jobs-reserved)
       :total-jobs (count jobs-of-tube)
       :current-waiting (count (:waiting_list @tube))
       :current-using (count (filter #(= (keyword name) (:use @%)) (vals @sessions)))
       :pause (:paused @tube)
       :cmd-pause-tube (:pauses @tube)
       :pause-time-left pause-time-left})))

;; stats command. Display server statistical data:
;;
;; * commands executions count
;; * jobs stats
;; * connections status, workers count, producer count.
;; * and more.
(defcommand "stats" [session]
  (let [all-jobs (vals @jobs)
        reserved-jobs (filter #(= :reserved (:state %)) all-jobs)
        ready-jobs (filter #(= :ready (:state %)) all-jobs)
        urgent-jobs (filter #(< (:priority %) 1024) ready-jobs)
        delayed-jobs (filter #(= :delayed (:state %)) all-jobs)
        buried-jobs (filter #(= :buried (:state %)) all-jobs)
        all-sessions (vals @sessions)
        worker-sessions (filter #(= :worker (:type @%)) all-sessions)
        waiting-sessions (filter #(= :waiting (:state @%)) worker-sessions)
        producer-sessions (filter #(= :producer (:type @%)) all-sessions)
        all-tubes (vals @tubes)
        commands-stats @commands]
;    (dbg commands-stats)
    (merge (zipmap (keys commands-stats) (map deref (vals commands-stats)))
           {:job-timeouts @job-timeouts
            :current-tubes (count all-tubes)
            :current-connections (count all-sessions)
            :current-producers (count producer-sessions)
            :current-workers (count worker-sessions)
            :current-waiting (count waiting-sessions)
            :uptime (int (/ (- (current-time) start-at) 1000))
            :current-jobs-urgent (count urgent-jobs)
            :current-jobs-ready (count ready-jobs)
            :current-jobs-reserved (count reserved-jobs)
            :current-jobs-delayed (count delayed-jobs)
            :current-jobs-buried (count buried-jobs)})))
  
;; ## Schedule tasks for time based work
;;

;; Set delay jobs as ready when they are timeout.
(defn- update-delay-job-for-tube [now tube]
  (dosync
    (let [ready-jobs (filter #(< (:deadline_at %) now) (:delay_set @tube))
          updated-jobs (map #(assoc % :state :ready) ready-jobs)]
      (doseq [job updated-jobs]        
        (alter tube assoc :delay_set (disj (:delay_set @tube) job))
        (set-job-as-ready job)))))

;; Loop all tubes to perform last function.
(defn update-delay-job-task []
  (doseq [tube (vals @tubes)] (update-delay-job-for-tube (current-time) tube)))

;; Release jobs that are exceed ttr
(defn update-expired-job-task []
  (dosync
    (let [reserved-jobs (filter #(= :reserved (:state %)) (vals @jobs))
          now (current-time)
          expired-jobs (filter #(> now (:deadline_at %)) reserved-jobs)]
      (doseq [job expired-jobs]
        (let [tube ((:tube job) @tubes)
              session (:reserver job)
              updated-job (assoc job :state :ready 
                                 :reserver nil
                                 :timeouts (inc (:timeouts job)))]
          (alter session assoc :reserved_jobs (disj (:reserved_jobs @session) (:id updated-job)))
          (alter job-timeouts inc)
          (set-job-as-ready updated-job))))))

;; Enable paused tubes when they are timeout
(defn update-paused-tube-task []
  (let [all-tubes (vals @tubes)
        paused-tubes (filter #(true? (:paused @%)) all-tubes)
        now (current-time)
        expired-tubes (filter #(> now (:pause_deadline @%)) paused-tubes)]
    (doseq [t expired-tubes]
      (dosync
        (alter t assoc :paused false))

        ;; handle waiting session
        (let [pending-pairs (zipmap (map @sessions (:waiting_list @t)) (:ready_set @t))]
          (doseq [s (keys pending-pairs)]
            (notify-waiting-session s (pending-pairs s)))))))

;; Update waiting workers when they are expired
;;
;; Find expired session (deadline_at is not nil and deadline_at is less
;; than now). Send a nil to the promise to release the blocking.
;;
;; !!IMPORTANT:
;; As timeout in deref is supported in clojure 1.3, this function will
;; be removed when clojalk and its dependencies ported to 1.3
(defn update-expired-waiting-session-task []
  (dosync
    (doseq
      [session (vals @sessions)]
      (if (= :waiting (:state @session))
        (let [now (current-time)
              deadline (:deadline_at @session)
              expired (and (not-nil deadline) (> now deadline))]
          (if (true? expired)
            (notify-waiting-session session nil)))))))

;; Start all tasks mentioned above with 5 threads.
;; Tasks are executed in fixed delay.
;;
;; A scheduler will be returned.
(defn start-tasks []
  (schedule-task 5 
                 [update-delay-job-task 0 1] 
                 [update-expired-job-task 0 1] 
                 [update-paused-tube-task 0 1]
                 [update-expired-waiting-session-task 0 1]))

;; Stop the scheduler
;;
(defn stop-tasks [scheduler]
  (.shutdownNow scheduler))
