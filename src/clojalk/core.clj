(ns clojalk.core
  (:refer-clojure :exclude [use peek])
  (:use [clojalk.utils]))

;; struct definition for Job
;; basic task unit
(defstruct Job :id :delay :ttr :priority :created_at :deadline_at :state :tube :body :reserver)

;; struct definition for Cube (similar to database in RDBMS)
(defstruct Tube :name :ready_set :delay_set :buried_list 
  :waiting_list :paused :pause_deadline)

;; struct definition for Session (connection in beanstalkd)
(defstruct Session :type :use :watch :deadline_at :state :incoming_job)

(defn- job-comparator [field j1 j2]
  (cond 
    (< (field j1) (field j2)) -1
    (> (field j1) (field j2)) 1
    :else (< (:id j1) (:id j2))))

(def priority-comparator
  (partial job-comparator :priority))

(def delay-comparator
  (partial job-comparator :delay))

(defn make-tube [name]
  (struct Tube (keyword name) ; name
          (ref (sorted-set-by priority-comparator)) ; ready_set
          (ref (sorted-set-by delay-comparator)) ; delay_set
          (ref []) ; buried_list
          (ref []) ; waiting queue
          (ref false) ; paused state
          (ref -1))) ; pause timeout

(defonce id-counter (atom 0))
(defn next-id []
  (swap! id-counter inc)) 

(defn make-job [priority delay ttr tube body]
  (let [id (next-id)
        now (current-time)
        created_at now
        state (if (> delay 0) :delayed :ready)]
    (struct Job id delay ttr priority created_at nil state tube body nil)))

(defn open-session [type]
  (ref (struct Session type :default #{:default} nil nil)))

;;------ clojalk globals -------

(defonce jobs (ref {}))
(defonce tubes (ref {:default (make-tube "default")}))
(defonce commands (ref {}))
(defonce start-at (current-time))

;;------ functions -------
(defn- top-ready-job [session]
  (let [watchlist (:watch @session)
        watch-tubes (filter not-nil (map #(get @tubes %) watchlist))
        watch-tubes (filter #(false? @(:paused %)) watch-tubes)
        top-jobs (filter not-nil (map #(first @(:ready_set %)) watch-tubes))]
    (first (apply sorted-set-by (conj top-jobs priority-comparator)))))

(defn- enqueue-waiting-session [session timeout]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name %)) (vals @tubes))
        deadline_at (if (nil? timeout) nil (+ (current-time) (* timeout 1000)))]
    (doseq [tube watch-tubes]
      (do
        (alter (:waiting_list tube) conj session)
        (alter session assoc 
               :state :waiting 
               :deadline_at deadline_at)
        (alter tubes assoc (:name tube) tube)))))

(defn- dequeue-waiting-session [session]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name %)) (vals @tubes))]
    (doseq [tube watch-tubes]
      (alter (:waiting_list tube) remove-item session)
      (alter session assoc :state :working)
      (alter tubes assoc (:name tube) tube))))

(defn- reserve-job [session job]
  (let [tube ((:tube job) @tubes)
        updated-top-job (assoc job
                               :state :reserved
                               :reserver session
                               :deadline_at (+ (current-time) (* (:ttr job) 1000)))]
    (do
      (alter (:ready_set tube) disj job)
      (alter jobs assoc (:id job) updated-top-job)
      (dequeue-waiting-session session)
      (alter session assoc :incoming_job updated-top-job)
      updated-top-job)))

(defn- set-job-as-ready [job]
  (let [tube ((:tube job) @tubes)]
    (do
      (alter jobs assoc (:id job) job)
      (alter (:ready_set tube) conj job)
      (if-let [s (first @(:waiting_list tube))]
        (reserve-job s job)))))

(defn- job-stats []
  {"current-jobs-urgent" (count (filter #(< (:priority %)) (vals @jobs)))
   "current-jobs-ready" (count (filter #(= :ready (:state %)) (vals @jobs)))
   "current-jobs-reserved" (count (filter #(= :reserved (:state %)) (vals @jobs)))
   "current-jobs-delayed" (apply + (map #(count @(:delay_set %)) (vals @tubes)))
   "current-jobs-buried" (apply + (map #(count @(:buried_list %)) (vals @tubes)))})

;;-------- macros ----------

(defmacro defcommand [name args & body]
  (dosync (alter commands assoc (str "cmd-" name) (atom 0)))
  `(defn ~(symbol name) ~args ~@body))

(defmacro exec-cmd [cmd & args]
  `(do
     (if-let [cnt# (get @commands (str "cmd-" ~cmd))]
       (swap! cnt# inc))
     (~(symbol cmd) ~@args)))

;;------ clojalk commands ------

(defcommand "put" [session priority delay ttr body]
  (let [tube ((:use @session) @tubes)
        job (make-job priority delay ttr (:name tube) body)]
    (do
      (dosync
        (case (:state job)
          :delayed (alter (:delay_set tube) conj job)
          :ready (set-job-as-ready job)))
      job)))

(defcommand "peek" [session id]
  (get @jobs id))

;; peek-* are producer tasks, peek job from current USED tubes (not watches)
(defcommand "peek-ready" [session]
  (let [tube ((:use @session) @tubes)]
    (first @(:ready_set tube))))

(defcommand "peek-delayed" [session]
  (let [tube ((:use @session) @tubes)]
    (first @(:delay_set tube))))

(defcommand "peek-buried" [session]
  (let [tube ((:use @session) @tubes)]
    (first @(:buried_list tube))))

(defcommand "reserve-with-timeout" [session timeout]
  (dosync
    (enqueue-waiting-session session timeout)
    (if-let [top-job (top-ready-job session)]
      (reserve-job session top-job))))

(defcommand "reserve" [session]
  (reserve-with-timeout session nil))

(defcommand "use" [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if-not (contains? @tubes tube-name-kw)
        (alter tubes assoc tube-name-kw (make-tube tube-name)))
        (alter session assoc :use tube-name-kw)
      session)))

(defcommand "delete" [session id]
  (if-let [job (get @jobs id)]
    (let [tube ((:tube job) @tubes)]
      (do
        (dosync
          (alter jobs dissoc id)
          (case (:state job)
            :ready (alter (:ready_set tube) disj job)
            :buried (alter (:buried_list tube) (fn [v i] (vec (remove-item v i))) job)
            () ;; default clause, do nothing
            )
          (alter session assoc :incoming_job nil)
          (alter session assoc :state :idle))
        (assoc job :state :invalid)))))

(defcommand "release" [session id priority delay]
  (if-let [job (get @jobs id)]
    (let [tube ((:tube job) @tubes)
          updated-job (assoc job :priority priority :delay delay)]
      (dosync
        (if (> delay 0)
          (alter (:delay_set tube) conj (assoc updated-job :state :delayed)) ;; delayed 
          (set-job-as-ready (assoc updated-job :state :ready)))
        (alter session assoc :incoming_job nil)
        (alter session assoc :state :idle)))))

(defcommand "bury" [session id priority]
  (if-let [job (get @jobs id)]
    (let [tube ((:tube job) @tubes)
          updated-job (assoc job :state :buried :priority priority)]
      (do
        (dosync
          ;; remove the job from ready_set and append it into buried_list
          ;; the job still can be found from jobs dictionary (for delete)
          (alter (:ready_set tube) disj job) ;;for reserved job, nothing done in this operation
          (alter (:buried_list tube) conj updated-job)
          (alter jobs assoc (:id updated-job) updated-job)
          (alter session assoc :incoming_job nil)
          (alter session assoc :state :idle))
        updated-job))))

;; for USED tube only
(defcommand "kick" [session bound]
  (let [tube ((:use @session) @tubes)]
    (dosync
      (if (empty? @(:buried_list tube))
        ;; no jobs buried, kick from delay set
        (let [kicked (take bound @(:delay_set tube))
              updated-kicked (map #(assoc % :state :ready) kicked)
              remained (drop bound @(:delay_set tube))
              remained-set (apply sorted-set-by delay-comparator remained)]
          
          (ref-set (:delay_set tube) remained-set)
          (doseq [job updated-kicked] (set-job-as-ready job)))
        
        ;; kick at most bound jobs from buried list
        (let [kicked (take bound @(:buried_list tube))
              updated-kicked (map #(assoc % :state :ready) kicked)
              remained (vec (drop bound @(:buried_list tube)))]
          (ref-set (:buried_list tube) remained)
          (doseq [job updated-kicked] (set-job-as-ready job)))))))

(defcommand "touch" [session id]
  (let [job (get @jobs id)
        updated-job (assoc job :deadline_at (+ (current-time) (* (:ttr job) 1000)))]
    (dosync
      (if (= :reserved (:state updated-job)) ;; only reserved jobs could be touched
        (do
          (alter jobs assoc (:id updated-job) updated-job)
          updated-job)))))

(defcommand "watch" [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if-not (contains? @tubes tube-name-kw)
        (alter tubes assoc tube-name-kw (make-tube tube-name)))
        (alter session assoc :watch (conj (:watch @session) tube-name-kw))
      session)))

(defcommand "ignore" [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (alter session assoc :watch (disj (:watch @session) tube-name-kw)))
    session))

(defcommand "list-tubes" [session]
  (keys @tubes))

(defcommand "list-tube-used" [session]
  (:use @session))

(defcommand "list-tubes-watched" [session]
  (:watch @session))

(defcommand "pause-tube" [session id timeout]
  (let [tube ((keyword id) @tubes)]
    (dosync
      (ref-set (:paused tube) true)
      (ref-set (:pause_deadline tube) (+ timeout (current-time)))
      (alter tubes assoc (:name tube) tube))))
  
;; ------- scheduled tasks ----------
(defn- update-delay-job-for-tube [now tube]
  (let [ready-jobs (filter #(< (+ (:created_at %) (* (:delay %) 1000)) now) @(:delay_set tube))
        updated-jobs (map #(assoc % :state :ready) ready-jobs)]
    (dosync
      (alter (:ready_set tube) conj-all updated-jobs)
      (alter (:delay_set tube) disj-all ready-jobs)
      (alter jobs merge (zipmap (map #(:id %) updated-jobs) updated-jobs)))))

(defn update-delay-job-task []
  (doseq [tube (vals @tubes)] (update-delay-job-for-tube (current-time) tube)))

(defn update-expired-job-task []
  (dosync
    (let [reserved-jobs (filter #(= :reserved (:state %)) (vals @jobs))
          now (current-time)
          expired-jobs (filter #(> now (:deadline_at %)) reserved-jobs)]
      (doseq [job expired-jobs]
        (let [tube ((:tube job) @tubes)]
          (alter jobs assoc (:id job) (assoc job :state :ready))
          (alter (:ready_set tube) conj (assoc job :state :ready)))))))
  
(defn update-paused-tube-task []
  (dosync
    (let [all-tubes (vals @tubes)
          paused-tubes (filter #(true? @(:paused %)) all-tubes)
          now (current-time)
          expired-tubes (filter #(> now @(:pause_deadline %)) paused-tubes)]
      (doseq [t expired-tubes]
        (do 
          (ref-set (:paused t) false)
          (alter tubes assoc (:name t) t)
          
          ;; handle waiting session
          (loop [s (first @(:waiting_list t))
                 j (first @(:ready_set t))]
            (if (and s j)
              (do
                (reserve-job s j)
                (recur (first @(:waiting_list t)) (first @(:ready_set t)))))))))))

(defn update-expired-waiting-session-task []
  (dosync
    (doseq
      [tube (vals @tubes)]
      (let [now (current-time)
            waiting_list @(:waiting_list tube)
            active-func (fn [s] (or (nil? (:deadline_at @s)) (< now (:deadline_at @s))))]
        (do
          (ref-set (:waiting_list tube) (vec (filter active-func waiting_list)))
          (doseq [session (filter #(false? (active-func %)) waiting_list)] 
            ;; we don't update deadline_at on this task, 
            ;; it will be updated next time when it's reserved
            (alter session assoc :state :idle))))))) 

