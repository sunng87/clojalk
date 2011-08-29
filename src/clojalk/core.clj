(ns clojalk.core
  (:refer-clojure :exclude [use peek])
  (:use [clojalk.utils]))

;; struct definition for Job
;; basic task unit
(defstruct Job :id :delay :ttr :priority :created_at 
  :deadline_at :state :tube :body :reserver
  :reserves :timeouts :releases :buries :kicks)

;; struct definition for Tube (similar to database in RDBMS)
(defstruct Tube :name :ready_set :delay_set :buried_list 
  :waiting_list :paused :pause_deadline :pauses)

;; struct definition for Session (connection in beanstalkd)
(defstruct Session :id :type :use :watch :deadline_at :state :incoming_job)

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
  (ref (struct Tube (keyword name) ; name
          (sorted-set-by priority-comparator) ; ready_set
          (sorted-set-by delay-comparator) ; delay_set
          [] ; buried_list
          [] ; waiting queue
          false ; paused state
          -1 ; pause timeout
          0))) ; pause command counter

(defonce id-counter (atom 0))
(defn next-id []
  (swap! id-counter inc)) 

(defn make-job [priority delay ttr tube body]
  (let [id (next-id)
        now (current-time)
        created_at now
        deadline_at (+ now (* 1000 delay))
        state (if (> delay 0) :delayed :ready)]
    (struct Job id delay ttr priority created_at 
            deadline_at state tube body nil
            0 0 0 0 0)))

(defn open-session 
  ([type] (open-session (uuid) type))
  ([id type] (ref (struct Session id type :default #{:default} nil nil))))

(defonce drain (atom false))
(defn toggle-drain []
  (swap! drain not))

;;------ clojalk globals -------

(defonce jobs (ref {}))
(defonce tubes (ref {:default (make-tube "default")}))
(defonce commands (ref {}))
(defonce start-at (current-time))

;; sessions 
(defonce sessions (ref {}))
(defonce job-timeouts (ref 0))

;;------ functions -------
(defn- top-ready-job [session]
  (let [watchlist (:watch @session)
        watch-tubes (filter not-nil (map #(get @tubes %) watchlist))
        watch-tubes (filter #(false? (:paused @%)) watch-tubes)
        top-jobs (filter not-nil (map #(first (:ready_set @%)) watch-tubes))]
    (first (apply sorted-set-by (conj top-jobs priority-comparator)))))

(defn- enqueue-waiting-session [session timeout]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name @%)) (vals @tubes))
        deadline_at (if (nil? timeout) nil (+ (current-time) (* timeout 1000)))]
    (doseq [tube watch-tubes]
      (do
        (alter tube assoc :waiting_list (conj (:waiting_list @tube) session))
        (alter session assoc 
               :state :waiting 
               :deadline_at deadline_at)))))

(defn- dequeue-waiting-session [session]
  (let [watch-tubes (filter #(contains? (:watch @session) (:name @%)) (vals @tubes))]
    (doseq [tube watch-tubes]
      (alter tube assoc :waiting_list (remove-item (:waiting_list @tube) session))
      (alter session assoc :state :working))))

(defn- reserve-job [session job]
  (let [tube ((:tube job) @tubes)
        deadline (if (zero? (:ttr job)) 
                   (Long/MAX_VALUE) (+ (current-time) (* (:ttr job) 1000)))
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
      updated-top-job)))

(defn- set-job-as-ready [job]
  (let [tube ((:tube job) @tubes)]
    (do
      (alter jobs assoc (:id job) job)
      (alter tube assoc :ready_set (conj (:ready_set @tube) job))
      (if-let [s (first (:waiting_list @tube))]
        (reserve-job s job)))))

;;-------- macros ----------

(defmacro defcommand [name args & body]
  (dosync (alter commands assoc (keyword (str "cmd-" name)) (atom 0)))
  `(defn ~(symbol name) ~args ~@body))

(defmacro exec-cmd [cmd & args]
  `(do
     (if-let [cnt# (get @commands (keyword (str "cmd-" ~cmd)))]
       (swap! cnt# inc))
     (~(symbol cmd) ~@args)))

;;------ clojalk commands ------

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

(defcommand "peek" [session id]
  (get @jobs id))

;; peek-* are producer tasks, peek job from current USED tubes (not watches)
(defcommand "peek-ready" [session]
  (let [tube ((:use @session) @tubes)]
    (first (:ready_set @tube))))

(defcommand "peek-delayed" [session]
  (let [tube ((:use @session) @tubes)]
    (first (:delay_set @tube))))

(defcommand "peek-buried" [session]
  (let [tube ((:use @session) @tubes)]
    (first (:buried_list @tube))))

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
    (if (and (contains? #{:reserved :buried} (:state job)) 
             (= (:id @(:reserver job)) (:id @session))) 
      (let [tube ((:tube job) @tubes)]
        (do
          (dosync
            (alter jobs dissoc id)
            (if (= (:state job) :buried)
              (alter tube assoc :buried_list 
                     (vec (remove-item (:buried_list @tube) job))))
            (alter session assoc :incoming_job nil)
            (alter session assoc :state :idle))
          (assoc job :state :invalid))))))

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
                (alter tube assoc 
                       :delay_set (conj (:delay_set @tube) (assoc updated-job :state :delayed))))
              (set-job-as-ready (assoc updated-job :state :ready)))
            (alter session assoc :incoming_job nil)
            (alter session assoc :state :idle))
          updated-job)))))

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
            (alter session assoc :state :idle))
          updated-job)))))

;; for USED tube only
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
  (if-let [tube ((keyword id) @tubes)]
    (dosync
      (alter tube assoc :paused true)
      (alter tube assoc :pause_deadline (+ (* timeout 1000) (current-time)))
      (alter tube assoc :pauses (inc (:pauses @tube))))))

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
  
;; ------- scheduled tasks ----------
(defn- update-delay-job-for-tube [now tube]
  (dosync
    (let [ready-jobs (filter #(< (:deadline_at %) now) (:delay_set @tube))
          updated-jobs (map #(assoc % :state :ready) ready-jobs)]
      (doseq [job updated-jobs]        
        (alter tube assoc :delay_set (disj (:delay_set @tube) job))
        (set-job-as-ready job)))))

(defn update-delay-job-task []
  (doseq [tube (vals @tubes)] (update-delay-job-for-tube (current-time) tube)))

(defn update-expired-job-task []
  (dosync
    (let [reserved-jobs (filter #(= :reserved (:state %)) (vals @jobs))
          now (current-time)
          expired-jobs (filter #(> now (:deadline_at %)) reserved-jobs)]
      (doseq [job expired-jobs]
        (let [tube ((:tube job) @tubes)
              updated-job (assoc job :state :ready 
                                 :reserver nil
                                 :timeouts (inc (:timeouts job)))]
          (alter jobs assoc (:id job) updated-job)
          (alter tube assoc :ready_set (conj (:ready_set @tube) updated-job))
          (alter job-timeouts inc))))))
  
(defn update-paused-tube-task []
  (dosync
    (let [all-tubes (vals @tubes)
          paused-tubes (filter #(true? (:paused @%)) all-tubes)
          now (current-time)
          expired-tubes (filter #(> now (:pause_deadline @%)) paused-tubes)]
      (doseq [t expired-tubes]
        (do 
          (alter t assoc :paused false)
          
          ;; handle waiting session
          (loop [s (first (:waiting_list @t))
                 j (first (:ready_set @t))]
            (if (and s j)
              (do
                (reserve-job s j)
                (recur (first (:waiting_list @t)) (first (:ready_set @t)))))))))))

(defn update-expired-waiting-session-task []
  (dosync
    (doseq
      [tube (vals @tubes)]
      (let [now (current-time)
            waiting_list (:waiting_list @tube)
            active-func (fn [s] (or (nil? (:deadline_at @s)) (< now (:deadline_at @s))))]
        (do
          (alter tube assoc :waiting_list (vec (filter active-func waiting_list)))
          (doseq [session (filter #(false? (active-func %)) waiting_list)] 
            ;; we don't update deadline_at on this task, 
            ;; it will be updated next time when it's reserved
            (alter session assoc :state :idle))))))) 

(defn start-tasks []
  (schedule-task 5 
                 [update-delay-job-task 0 1] 
                 [update-expired-job-task 0 1] 
                 [update-paused-tube-task 0 1]
                 [update-expired-waiting-session-task 0 1]))

(defn stop-tasks [scheduler]
  (.shutdownNow scheduler))
