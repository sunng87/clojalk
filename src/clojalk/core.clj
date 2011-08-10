(ns clojalk.core
  (:use [clojalk.utils]))

;; struct definition for Job
;; basic task unit
(defstruct JobSpec :id :ttr :priority :created_at :activated_at :state)
(defstruct Job :jobspec :tube :body)

;; struct definition for Cube (similar to database in RDBMS)
(defstruct Tube :name :ready_set :delay_set)

;; struct definition for Session (connection in beanstalkd)
(defstruct Session :type :use :watches)

(defn- priority-comparator [j1 j2]
  (< (:priority (:jobspec j1)) (:priority (:jobspec j2))))

(defn make-tube [name]
  (struct Tube name 
          (ref (sorted-set-by priority-comparator))
          (ref (sorted-set-by priority-comparator))))

(defonce id-counter (atom 0))
(defn next-id []
  (swap! id-counter inc))

(defn make-job [priority delay ttr]
  (let [id (next-id)
        created_at (current-time)
        activated_at (+ (current-time) delay)
        state (if (< created_at activated_at) :delay :ready)]
    (struct JobSpec id ttr priority created_at activated_at state)))

(defn open-session [type]
  (struct Session type :default #{}))

;;------ clojalk globals -------

(defonce jobs (ref {}))
(defonce tubes (ref {:default (make-tube "default")}))

;;------ clojalk commands ------

(defn put [session priority delay ttr body]
  (let [tube ((:use session) @tubes)
        jobspec (make-job priority delay ttr)
        job (struct Job jobspec (:name tube) body)]
    (dosync
      (alter jobs assoc (:id jobspec) job)
      (case (:state jobspec)
        :delay (alter (:delay_set tube) conj job)
        :ready (alter (:ready_set tube) conj job)))))

