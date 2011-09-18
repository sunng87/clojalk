;; # Clojalk WAL module
;;
;; WAL module provides persistent facility for clojalk.
;; Jobs are log into a sequenced binary file when updated. The file will be
;; replayed when clojalk restarting.
;;

(ns clojalk.wal
  (:refer-clojure :exclude [use peek])
  (:require clojalk.data)
  (:use [clojalk utils])
  (:use clojure.java.io)
  (:import [java.nio ByteBuffer])
  (:import [java.io FileOutputStream]))

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
  (if (nil? (:tube job)) (println job))
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
      (if-not (zero? (.available s))
        (do
          (if-let [job (read-job s)]
            (handler job))
          (recur s))))))

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
  (not-nil (:tube j)))

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
(defn- replay-handler [j]
  (if (is-full-record j)
    (alter clojalk.data/jobs assoc (:id j) j)
    (if (= :invalid (:state j))
      (alter clojalk.data/jobs dissoc (:id j))
      (let [id (:id j)
            jr (if (= :reserved (:state j)) (assoc j :state :ready) j)]
        (alter clojalk.data/jobs assoc id
          (merge-with #(if (nil? %2) %1 %2) (@clojalk.data/jobs id) jr))))))

;; Construct tube data structures. Load job references into certain container of
;; tube. Create a new tube if not found.
;;
(defn- replay-tubes []
  (doseq [jr (vals @clojalk.data/jobs)]
    (let [tube (@clojalk.data/tubes (:tube jr))]
      (if (nil? tube)
        (alter clojalk.data/tubes assoc (:tube jr) (clojalk.data/make-tube (:tube jr)))))
    (let [tube (@clojalk.data/tubes (:tube jr))]
      (case (:state jr)
        :ready (alter tube assoc :ready_set (conj (:ready_set @tube) jr))
        :buried (alter tube assoc :buried_lsit (conj (:buried_lsit @tube) jr))
        :delayed (alter tube assoc :delay_set (conj (:delay_set @tube) jr))))))

;; Update id counter after all job records are loaded from logs
;;
;; IMPORTANT! Append a **0** into the job key collection to prevent
;; exception when there is no jobs
(defn- update-id-counter []
  (swap! clojalk.data/id-counter
    (constantly (long  (apply max (conj (keys @clojalk.data/jobs) 0))))))

;; ## Replay logs and load jobs
;;
;; Read logs files from configured directory, load job records from them.
;; Jobs will be reloaded into memory. Job body and tube name won't be overwrite when records
;; with same id are found because there will be only one full record for each job, which is
;; also the first record for it.
;; After all jobs are loaded into `clojalk.data/jobs`, we will update their references in
;; each tube (ready_set, delay_set and bury_list). (Tubes are created if not found.)
;;
;; After all done, remove the log files.
;;
;; All the statistical information about commands invocation are lost when
;; server restarted.
;;
(defn replay-logs []
  (if-let [bin-log-files (scan-dir *clojalk-log-dir*)]
    (do
      (dosync
        (doall (map #(read-file % replay-handler) bin-log-files))
        (replay-tubes))
      (println (str (count @clojalk.data/jobs) " jobs loaded from write-ahead logs."))
      (update-id-counter)))
  (empty-dir *clojalk-log-dir*))

;; log files are split into several parts
;; this var is referenced only when initializing files
(def *clojalk-log-count* 8)

;; log file streams
(def log-files (ref []))

;; Create empty log files into `log-files`. This is invoked after legacy logs replayed.
(defn init-log-files []
  (let [dir (file *clojalk-log-dir*)]
    (if-not (.exists dir) (.mkdirs dir))
    (if-not (.exists dir)
      (throw (IllegalStateException.
               (str "Failed to create WAL directory: " (.getAbsolutePath dir))))))
  (dosync
    (loop [i 0]
      (if-not (= i *clojalk-log-count*)
        (do
          (alter log-files conj 
                 (agent (FileOutputStream. 
                          (file *clojalk-log-dir* (str "clojalk-" i ".bin")) true)))
          (recur (inc i)))))))
  
;; A flag for enable/disable WAL
(def *clojalk-log-enabled* false)

;; A convenience function to write and flush stream
(defn- stream-write [s data]
  (do
    (.write s data)
    s))

;; Write the job record into certain log stream.
;; Here we use a `mod` function to hash job id into a log stream index.
;;
;; We should test if WAL is properly initialized before we actually write
;; logs.
;;
(defn write-job [j full?]
  (if (not-empty @log-files)
    (let [id (:id j)
          log-files-count (count @log-files)
          log-file-index (mod id log-files-count)
          log-stream (nth @log-files log-file-index)
          job-bytes (.array (job-to-bin j full?))]
      (send log-stream stream-write job-bytes))))

;; Write all jobs into log streams as full record
(defn dump-all-jobs []
  (doseq [j (vals @clojalk.data/jobs)]
    (write-job j true)))

;; Start proceduce of WAL module invoked before server and task start
(defn start-wal []
  (if *clojalk-log-enabled*
    (do
      (replay-logs)
      (init-log-files)
      (dump-all-jobs))))
