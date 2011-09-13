(ns clojalk.wal
  (:use clojure.java.io)
  (:import [java.nio ByteBuffer IntBuffer]))

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
        tube-name-length (.get (IntBuffer/wrap (read-bytes stream 4)))
        tube-name (if (zero? tube-name-length) 
                    nil 
                    (String. (read-bytes stream tube-name-length) "UTF8"))
        job-body-length (.get (IntBuffer/wrap (read-bytes stream 4)))
        job-body (if (zero? job-body-length) 
                   nil 
                   (String. (read-bytes stream tube-name-length) "UTF8"))
        
        job (transient {})]
    (assoc! job :id (.getLong base-bytes))
    (assoc! job :delay (.getInt base-bytes))
    (assoc! job :ttr (.getInt base-bytes))
    (assoc! job :priority (.getInt base-bytes))
    (assoc! job :created_at (.getLong base-bytes))
    (assoc! job :deadline_at (.getLong base-bytes))
    (assoc! job :state (enum-state (.getShort base-bytes)))
    (assoc! job :reserves (.getInt base-bytes))
    (assoc! job :timeouts (.getInt base-bytes))
    (assoc! job :releases (.getInt base-bytes))
    (assoc! job :buries (.getInt base-bytes))
    (assoc! job :kicks (.getInt base-bytes))
    (if-not (nil? tube-name) (assoc! job :tube tube-name))
    (if-not (nil? job-body) (assoc! job :body job-body))
    (persistent! job)))

;; Read a bin file into a vector of job entries
(defn read-file [bin-log-file]
  (with-open [stream (input-stream bin-log-file)]
    (loop [s stream jobs []]
      (if (zero? (.available s))
        jobs
        (recur s (conj jobs (read-job s)))))))

;; Scan directory to find files whose name endswith .bin
(defn scan-dir [dir-path]
  (filter #(.endsWith (.getName %) ".bin") (.listFiles (file dir-path))))

;; default clojalk log directory
(def *clojalk-log-dir*)

;; Replay logs and load jobs
;; 
(defn replay-logs []
  (let [bin-log-files (scan-dir *clojalk-log-dir*)]
    (doseq [bin-log-file bin-log-files]
      (read-file bin-log-file)))) ;;TODO
