(ns clojalk.core
  (:use [clojalk.utils]))

;; struct definition for Job
;; basic task unit
(defstruct JobSpec :id :ttr :priority :created_at :activated_at :state)
(defstruct Job :jobspec :cube :body)

;; struct definition for Cube (similar to database in RDBMS)
(defstruct Cube :name :ready_set :delay_set)

;; struct definition for Session (connection in beanstalkd)
(defstruct Session :type :use :watches)

(defn- priority-comparator [j1 j2]
  (< (:priority j1) (:priority j2)))

(defn make-cube [name]
  (struct Cube name 
          (sorted-set-by priority-comparator) 
          (sorted-set-by priority-comparator)))

(def cubes (ref {:default (make-cube "default")}))

(defonce id-counter (atom 0))
(defn next-id []
  (swap! id-counter inc))

(defn make-job [priority delay ttr]
  (let [id (next-id)
        created_at (current-time)
        activated_at (+ (current-time) delay)
        state (if (< created_at activated_at) :delay :ready)]
    (struct JobSpec id ttr priority created_at activated_at state)))

;;------ clojalk commands ------

(defn put [session priority delay ttr body]
  (let [jobspec (make-job priority delay ttr)
        job (struct Job jobspec (:cube session) body)]
    ))

