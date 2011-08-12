(ns clojalk.core
  (:use [clojalk.utils]))

;; struct definition for Job
;; basic task unit
(defstruct Job :id :delay :ttr :priority :created_at :deadline_at :state :tube :body :reserver)

;; struct definition for Cube (similar to database in RDBMS)
(defstruct Tube :name :ready_set :delay_set :buried_list)

;; struct definition for Session (connection in beanstalkd)
(defstruct Session :type :use :watch)

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
  (struct Tube (keyword name)
          (ref (sorted-set-by priority-comparator))
          (ref (sorted-set-by delay-comparator))
          (ref [])))

(defonce id-counter (atom 0))
(defn next-id []
  (swap! id-counter inc)) ;;convert to string

(defn make-job [priority delay ttr tube body]
  (let [id (next-id)
        created_at (current-time)
        activated_at (+ (current-time) delay)
        state (if (< created_at activated_at) :delayed :ready)]
    (struct Job id delay ttr priority created_at nil state tube body nil)))

(defn open-session [type]
  (struct Session type :default #{:default}))

;;------ clojalk globals -------

(defonce jobs (ref {}))
(defonce tubes (ref {:default (make-tube "default")}))
(defonce commands (ref {}))

;;------ clojalk commands ------

(defn put [session priority delay ttr body]
  (let [tube ((:use session) @tubes)
        job (make-job priority delay ttr (:name tube) body)]
    (do
      (dosync
        (case (:state job)
          :delayed (alter (:delay_set tube) conj job)
          :ready (do
                   (alter jobs assoc (:id job) job)
                   (alter (:ready_set tube) conj job))))
      job)))

(defn peek [session id]
  (get @jobs id))

;; peek-* are producer tasks, peek job from current USED tubes (not watches)
(defn peek-ready [session]
  (let [tube ((:use session) @tubes)]
    (first @(:ready_set tube))))

(defn peek-delayed [session]
  (let [tube ((:use session) @tubes)]
    (first @(:delay_set tube))))

(defn peek-buried [session]
  (let [tube ((:use session) @tubes)]
    (first @(:buried_list tube))))

(defn reserve [session]
  (let [watchlist (:watch session)
        watch-tubes (filter not-nil (map #(get @tubes %) watchlist))
        top-jobs (filter not-nil (map #(first @(:ready_set %)) watch-tubes))
        top-job (first (apply sorted-set-by (conj top-jobs priority-comparator)))
        updated-top-job (if top-job 
                          (assoc top-job
                                 :state :reserved
                                 :reserver session
                                 :deadline_at (+ (current-time) (* (:ttr top-job) 1000))))
        top-job-cube (and top-job (get @tubes (:tube top-job)))]
    (if top-job
      (do
        (dosync 
          (alter (:ready_set top-job-cube) disj top-job)
          (alter jobs assoc (:id top-job) updated-top-job))
        updated-top-job))))

(defn use [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if-not (contains? @tubes tube-name-kw)
        (alter tubes assoc tube-name-kw (make-tube tube-name))))
    (assoc session :use tube-name-kw)))

(defn delete [session id]
  (if-let [job (get @jobs id)]
    (let [tube ((:tube job) @tubes)]
      (do
        (dosync
          (alter jobs dissoc id)
          (case (:state job)
            :ready (alter (:ready_set tube) disj job)
            :buried (alter (:buried_list tube) (fn [v i] (vec (remove-item v i))) job)
            () ;; default clause, do nothing
            ))
        (assoc job :state :invalid)))))

(defn release [session id priority delay]
  (if-let [job (get @jobs id)]
    (let [tube ((:tube job) @tubes)
          updated-job (assoc job :priority priority :delay delay)]
      (dosync
        (if (> delay 0)
          (alter (:delay_set tube) conj (assoc updated-job :state :delayed)) ;; delayed 
          (alter (:ready_set tube) conj (assoc updated-job :state :ready)))))))

(defn bury [session id priority]
  (if-let [job (get @jobs id)]
    (let [tube ((:tube job) @tubes)
          updated-job (assoc job :state :buried :priority priority)]
      (do
        (dosync
          ;; remove the job from ready_set and append it into buried_list
          ;; the job still can be found from jobs dictionary (for delete)
          (alter (:ready_set tube) disj job) ;;for reserved job, nothing done in this operation
          (alter (:buried_list tube) conj updated-job)
          (alter jobs assoc (:id updated-job) updated-job))
        updated-job))))

(defn watch [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (dosync
      (if-not (contains? @tubes tube-name-kw)
        (alter tubes assoc tube-name-kw (make-tube tube-name))))
    (assoc session :watch (conj (:watch session) tube-name-kw))))

(defn ignore [session tube-name]
  (let [tube-name-kw (keyword tube-name)]
    (assoc session :watch (disj (:watch session) tube-name-kw))))

(defn list-tubes [session]
  (keys @tubes))

(defn list-tube-used [session]
  (:use session))

(defn list-tubes-watched [session]
  (:watch session))
  
;; ------- scheduled tasks ----------
(defn- update-delay-job-for-tube [now tube]
  (let [ready-jobs (filter #(< (+ (:created_at %) (* (:delay %) 1000)) now) @(:delay_set tube))
        updated-jobs (map #(assoc % :state :ready) ready-jobs)]
    (if-not (empty? updated-jobs)
      (dosync
        (alter (:ready_set tube) conj-all updated-jobs)
        (alter (:delay_set tube) disj-all ready-jobs)
        (alter jobs merge (zipmap (map #(:id %) updated-jobs) updated-jobs))))))

(defn update-delay-job-task []
  (doseq [tube (vals @tubes)] (update-delay-job-for-tube (current-time) tube)))

