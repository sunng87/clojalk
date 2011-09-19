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
  (:use [clojalk data utils])
  (:require [clojalk.wal]))



;; ## Functions to handle clojalk logic

;; Find top priority job from session's watch list. Steps:
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
;; (This function does not open transaction so it should run within a dosync block)
(defn- enqueue-waiting-session [session timeout]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name @%)) (vals @tubes))
        deadline_at (if (nil? timeout) nil (+ (current-time) (* timeout 1000)))]
    (doseq [tube watch-tubes]
      (alter tube assoc :waiting_list (conj (:waiting_list @tube) session)))
    (alter session assoc
               :state :waiting
               :deadline_at deadline_at)))

;; Remove session from waiting_list of all tubes it watches.
;; This function is invoked when a session successfully reserved a job.
;; This also updates session *state* to `working` and leave *deadline_at* as it is.
;;
;; (This function does not open transaction so it should run within a dosync block)
(defn- dequeue-waiting-session [session]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name @%)) (vals @tubes))]
    (doseq [tube watch-tubes]
      (alter tube assoc :waiting_list (into [] (remove-item (:waiting_list @tube) session))))
    (alter session assoc :state :working)))

;; Reserve the job with the session. Steps:
;;
;; 1. Find tube of this job
;; 1. Compute deadline of this reservation
;; 1. Create an updated version of job
;;   - set state to `reserved`
;;   - set reserver to this session
;;   - set deadline_at to deadline of last step
;;   - increase reserve count
;; 1. Remove ths job from its tube's ready_set
;; 1. Update job in @jobs
;; 1. Run `dequeue-waiting-session` on this session
;; 1. Assign the job to `incoming_job` of the session
;; 1. Append the job id to `reserved_jobs` of the session
;;
;; Finally, this function returns the reserved job.
;;
;; (This function does not open transaction so it should run within a dosync block)
(defn- reserve-job [session job]
  (let [tube ((:tube job) @tubes)
        deadline (+ (current-time) (* (:ttr job) 1000))
        updated-top-job (assoc job
                               :state :reserved
                               :reserver session
                               :deadline_at deadline
                               :reserves (inc (:reserves job)))]
    (do
      (alter tube assoc :ready_set (disj (:ready_set @tube) job))
      (alter jobs assoc (:id job) updated-top-job)
      (dequeue-waiting-session session)
      (alter session assoc :incoming_job updated-top-job)
      (alter session assoc :reserved_jobs 
             (conj (:reserved_jobs @session) (:id updated-top-job)))
      (clojalk.wal/write-job updated-top-job false)
      updated-top-job)))

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
      (if-let [s (first (:waiting_list @tube))]
        (reserve-job s job)))))

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
    (let [session (ref (struct Session id type :default #{:default} nil :idle nil #{}))]
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
      (dequeue-waiting-session session)
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
        (clojalk.wal/write-job job true)
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
;; tubes. If there is no job ready for reservation, it will wait at most `timeout`
;; seconds.
;; BE CAUTION: this is only for server mode. If you use clojalk as a embedded library,
;; `reserve-with-timeout` will return nil at once if there is no job ready.
(defcommand "reserve-with-timeout" [session timeout]
  (dosync
    (enqueue-waiting-session session timeout)
    (if-let [top-job (top-ready-job session)]
      (reserve-job session top-job))))

;; `reserve` is a worker task. It will wait for available jobs without timeout.
;; BE CAUTION: this is only for server mode. If you use clojalk as a embedded library,
;; `reserve` will return nil at once if there is no job ready.
;;
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
;; 5. Empty the incoming_job field of session, remove the job from
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
            (alter session assoc :incoming_job nil)
            (alter session assoc :reserved_jobs 
                   (disj (:reserved_jobs @session) (:id job)))
            (if (empty? (:reserved_jobs @session))
              (alter session assoc :state :idle)))
          (clojalk.wal/write-job (assoc job :state :invalid) false)
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
            (alter session assoc :incoming_job nil)
            (alter session assoc :reserved_jobs 
                   (disj (:reserved_jobs @session) (:id updated-job)))
            (if (empty? (:reserved_jobs @session))
              (alter session assoc :state :idle)))
          (clojalk.wal/write-job updated-job false)
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
            (alter session assoc :incoming_job nil)
            (alter session assoc :reserved_jobs 
                   (disj (:reserved_jobs @session) (:id updated-job)))
            (if (empty? (:reserved_jobs @session))
              (alter session assoc :state :idle)))
          (clojalk.wal/write-job updated-job false)
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
              updated-kicked (map #(assoc % :state :ready :kicks (inc (:kicks %))) kicked)
              remained (drop bound (:delay_set @tube))
              remained-set (apply sorted-set-by delay-comparator remained)]
          
          (alter tube assoc :delay_set remained-set)
          (doseq [job updated-kicked]
            (clojalk.wal/write-job job false)
            (set-job-as-ready job))
          updated-kicked)
        
        ;; kick at most bound jobs from buried list
        (let [kicked (take bound (:buried_list @tube))
              updated-kicked (map #(assoc % :state :ready :kicks (inc (:kicks %))) kicked)
              remained (vec (drop bound (:buried_list @tube)))]
          (alter tube assoc :buried_list remained)
          (doseq [job updated-kicked]
            (clojalk.wal/write-job job false)
            (set-job-as-ready job))
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
        commands-stats @commands
        commands-stats-keys (keys commands-stats)]
;    (dbg commands-stats)
    (merge (zipmap commands-stats-keys (map #(deref (commands-stats %)) commands-stats-keys))
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
        (clojalk.wal/write-job job false)
        (set-job-as-ready job)))))

;; Loop all tubes to perform last function.
(defn update-delay-job-task []
  (doseq [tube (vals @tubes)] (update-delay-job-for-tube (current-time) tube)))

;; Release jobs that are exceed ttr
(defn update-expired-job-task []
  (let [reserved-jobs (filter #(= :reserved (:state %)) (vals @jobs))
        now (current-time)
        expired-jobs (filter #(> now (:deadline_at %)) reserved-jobs)]
    (doseq [job expired-jobs]
      (let [tube ((:tube job) @tubes)
            session (:reserver job)
            updated-job (assoc job :state :ready
                               :reserver nil
                               :timeouts (inc (:timeouts job)))]
        (clojalk.wal/write-job updated-job false)
        (dosync
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
        (alter t assoc :paused false)
        
        ;; handle waiting session
        (let [pending-pairs (zipmap (:waiting_list @t) (:ready_set @t))]
          (doseq [s (keys pending-pairs)]
            (reserve-job s (pending-pairs s))))))))

;; Update waiting workers when they are expired
;;
;; we don't update deadline_at on this task,
;; it will be updated next time when it's reserved
(defn update-expired-waiting-session-task []
  (dosync
    (doseq
      [session (vals @sessions)]
      (if (= :waiting (:state @session))
        (let [now (current-time)
              deadline (:deadline_at @session)
              expired (and (not-nil deadline) (> now deadline))]
          (if (true? expired)
            (do
              (dequeue-waiting-session session)
              (alter session assoc :state :idle))))))))

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
