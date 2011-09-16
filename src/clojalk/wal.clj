;; # Clojalk WAL module
;;
;; WAL module provides persistent facility for clojalk.
;; Jobs are log into a sequenced binary file when updated. The file will be
;; replayed when clojalk restarting.
;;

(ns clojalk.wal
  (:refer-clojure :exclude [use peek])
  (:require clojalk.core)
  (:use [clojalk utils])
  (:use clojure.java.io)
  (:import [java.nio ByteBuffer]))

(def job-base-size 58)

(defn- as-bytes [s]
  (.getBytes s "UTF8"))

(defn state-enum [state]
  (short (case state
    :ready 0
    :delayed 1
    :reserved 2
    :buried 3
    :invalid 4
    -1)))

(defn enum-state [e]
  (nth [:ready :delayed :reserved :buried :invalid] e))

;;
;; Write a job record into a ByteBuffer
;; The record contains:
;; 1. id - 8 bytes
;; 2. delay - 4 bytes
;; 3. ttr - 4 bytes
;; 4. priority - 4 bytes
;; 5. created_at - 8 bytes
;; 6. deadline_at - 8 bytes
;; 7. state - 2 bytes
;; 8. reserves - 4 bytes
;; 9. timeouts - 4 bytes
;; 10. releases - 4 bytes
;; 11. buries - 4 bytes
;; 12. kicks - 4 bytes
;; 13. tube-name-length - 4 bytes 
;; 14. tube-name - tube-name-length bytes
;; 15. body-length - 4 bytes
;; 16. body - body-length bytes
;;
;; If not in full mode, tube-name and body will not be wrote into buffer.
;;
(defn job-to-bin [job full]
  (let [tube-name-bytes (as-bytes (name (:tube job)))
        job-body-bytes (as-bytes (:body job))
        byte-length (if full 
                      (+ job-base-size 
                         4 (alength tube-name-bytes) 
                         4 (alength job-body-bytes)) 
                      (+ job-base-size 4 4))
        buffer (ByteBuffer/allocate byte-length)]
    (-> buffer
        (.putLong (long (:id job)))
        (.putInt (int (:delay job)))
        (.putInt (int (:ttr job)))
        (.putInt (int (:priority job)))
        (.putLong (long (:created_at job)))
        (.putLong (long (or (:deadline_at job) 0)))
        (.putShort (state-enum (:state job)))
        (.putInt (int (:reserves job)))
        (.putInt (int (:timeouts job)))
        (.putInt (int (:releases job)))
        (.putInt (int (:buries job)))
        (.putInt (int (:kicks job))))
    (if full
      (do
        (.putInt buffer (alength tube-name-bytes))
        (.put buffer tube-name-bytes)
        (.putInt buffer (alength job-body-bytes))
        (.put buffer job-body-bytes))
      (do
        (.putInt buffer 0)
        (.putInt buffer 0)))
    buffer))

;; read a fixed size of bytes from stream
(defn- read-bytes [stream size]
  (let [bytes (byte-array size)]
    (do
      (.read stream bytes)
      bytes)))

;; Read a job entry from stream
;; To test if a job entry is a full entry, test if its :tube is not nil
;; 
;; I use a transient map here to simplify the code and improve performance
(defn read-job [stream]
  (let [base-bytes (ByteBuffer/wrap (read-bytes stream job-base-size))
        tube-name-length (.getInt (ByteBuffer/wrap (read-bytes stream 4)))
        tube-name (if-not (zero? tube-name-length)
                    (keyword (String. (read-bytes stream tube-name-length) "UTF8")))
        job-body-length (.getInt (ByteBuffer/wrap (read-bytes stream 4)))
        job-body (if-not (zero? job-body-length) 
                   (String. (read-bytes stream job-body-length) "UTF8"))]
    (assoc
      {}
      :id (.getLong base-bytes)
      :delay (.getInt base-bytes)
      :ttr (.getInt base-bytes)
      :priority (.getInt base-bytes)
      :created_at (.getLong base-bytes)
      :deadline_at (.getLong base-bytes)
      :state (enum-state (.getShort base-bytes))
      :reserves (.getInt base-bytes)
      :timeouts (.getInt base-bytes)
      :releases (.getInt base-bytes)
      :buries (.getInt base-bytes)
      :kicks (.getInt base-bytes)
      :tube tube-name
      :body job-body)))

;; Read a bin file into a vector of job entries
(defn read-file [bin-log-file handler]
  (with-open [stream (input-stream bin-log-file)]
    (loop [s stream]
      (let [job (read-job s)]
        (handler job))
      (if-not (zero? (.available s))
        (recur s)))))

;; Scan directory to find files whose name ends with .bin
(defn scan-dir [dir-path]
  (filter #(.endsWith (.getName %) ".bin") (.listFiles (file dir-path))))

;; Delete logging files under the dir
(defn empty-dir [dir-path]
  (doseq [f (scan-dir dir-path)]
    (delete-file f)))

;; default clojalk log directory, to be overwrite by configuration
(def *clojalk-log-dir* "./binlogs/")

;; Test if a jobrec is a full record
(defn is-full-record [j]
  (not (nil? (:tube j))))

;; Load a job record into memory
;;
;; 1. Test if the job record is a full record.
;; 2. True: Add the full record to jobs
;; 3. False: Merge the record with new one / or just remove the record (`:invalid` state)
;;
;; Merge Strategy:
;; 1. if the job record is `:reserved`, just reset it to `:ready`
;; 2. Merge all fields of the record except tube-name and job-body
;; (which are not stored in non-full record)
;;
(defn replay-handler [j]
  (if (is-full-record j)
    (alter clojalk.core/jobs assoc (:id j) j)
    (if (= :invalid (:state j))
      (alter clojalk.core/jobs dissoc (:id j))
      (let [id (:id j)
            jr (if (= :reserved (:state j)) (assoc j :state :ready) j)]
        (alter clojalk.core/jobs assoc id
          (merge-with #(if (nil? %2) %1 %2) (@clojalk.core/jobs id) jr))))))

;; Construct tube data structures. Load job references into certain container of
;; tube. Create a new tube if not found.
;;
(defn- replay-tubes []
  (doseq [jr (vals @clojalk.core/jobs)]
    (let [tube (@clojalk.core/tubes (:tube jr))]
      (if (nil? tube)
        (alter clojalk.core/tubes assoc (:tube jr) (clojalk.core/make-tube (:tube jr))))
      (case (:state jr)
        :ready (alter tube assoc :ready_set (conj (:ready_set @tube) jr))
        :buried (alter tube assoc :buried_lsit (conj (:buried_lsit @tube) jr))
        :delayed (alter tube assoc :delay_set (conj (:delay_set @tube) jr))))))

(defn- update-id-counter []
  (swap! clojalk.core/id-counter (constantly (long  (apply max (keys @clojalk.core/jobs))))))

;; ## Replay logs and load jobs
;;
;; Read logs files from configured directory, load job records from them.
;; Jobs will be reloaded into memory. Job body and tube name won't be overwrite when records
;; with same id are found because there will be only one full record for each job, which is
;; also the first record for it.
;; After all jobs are loaded into `clojalk.core/jobs`, we will update their references in
;; each tube (ready_set, delay_set and bury_list). (Tubes are created if not found.)
;;
;; After all done, remove the log files.
;;
;; All the statistical information about commands invocation are lost when
;; server restarted.
;;
(defn replay-logs []
  (let [bin-log-files (scan-dir *clojalk-log-dir*)]
    (dosync
      (doall (map #(read-file % replay-handler) bin-log-files))
      (replay-tubes)))
  (update-id-counter)
  (empty-dir *clojalk-log-dir*))
