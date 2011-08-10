(ns clojalk.core
  (:use [clojalk.utils]))

;; struct definition for Job
;; basic task unit
(defstruct Job :id :ttr :priority :created_at :deadline_at :state :tube :body :reserver)

;; struct definition for Cube (similar to database in RDBMS)
(defstruct Tube :name :ready_set :delay_set)

;; struct definition for Session (connection in beanstalkd)
(defstruct Session :type :use :watches)

(defn- priority-comparator [j1 j2]
  (cond 
    (< (:priority j1) (:priority j2)) -1
    (> (:priority j1) (:priority j2)) 1
    :else (< (:id j1) (:id j2))))

(defn make-tube [name]
  (struct Tube (keyword name)
          (ref (sorted-set-by priority-comparator))
          (ref (sorted-set-by priority-comparator))))

(defonce id-counter (atom 0))
(defn next-id []
  (swap! id-counter inc)) ;;convert to string

(defn make-job [priority delay ttr tube body]
  (let [id (next-id)
        created_at (current-time)
        activated_at (+ (current-time) delay)
        state (if (< created_at activated_at) :delay :ready)]
    (struct Job id ttr priority created_at nil state tube body nil)))

(defn open-session [type]
  (struct Session type :default #{:default}))

;;------ clojalk globals -------

(defonce jobs (ref {}))
(defonce tubes (ref {:default (make-tube "default")}))

;;------ clojalk commands ------

(defn put [session priority delay ttr body]
  (let [tube ((:use session) @tubes)
        job (make-job priority delay ttr (:name tube) body)]
    (do
      (dosync
        (alter jobs assoc (:id job) job)
        (case (:state job)
          :delay (alter (:delay_set tube) conj job)
          :ready (alter (:ready_set tube) conj job)))
      job)))

(defn reserve [session]
  (let [watchlist (:watches session)
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
            :delay (alter (:delay_set tube) disj job)
            () ;; default clause, do nothing
            ))
        (assoc job :state :invalid)))))


