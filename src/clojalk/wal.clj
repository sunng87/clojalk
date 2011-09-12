(ns clojalk.wal
  (:import [java.nio ByteBuffer]))

(def job-base-size 58)

(defn- as-bytes [s]
  (.getBytes s "UTF8"))

(defn- state-enum [state]
  (short (case state
    :ready 0
    :delayed 1
    :reserved 2
    :buried 3
    :invalid 4
    -1)))

(defn- enum-state [e]
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



