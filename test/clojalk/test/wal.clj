(ns clojalk.test.wal
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core wal utils])
  (:use [clojure.test]))

(def job (struct Job 100 0 1000 1023 
                 0 nil :ready
                 :default "tomcat" nil 0 0 0 0 0))

(defn- getString [buf length]
  (let [bytes (byte-array length)]
    (.get buf bytes)
    (String. bytes "UTF8")))

(deftest test-job-to-bin
  (let [buffer (job-to-bin job true)]
    (.rewind buffer)
    (are [x y] (= x y)
         100 (.getLong buffer)
         0 (.getInt buffer)
         1000 (.getInt buffer)
         1023 (.getInt buffer)
         0 (.getLong buffer)
         0 (.getLong buffer)
         :ready (enum-state (.getShort buffer))
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         7 (.getInt buffer)
         "default" (getString buffer 7)
         6 (.getInt buffer)
         "tomcat" (getString buffer 6)))
         
  (let [buffer (job-to-bin job false)]
    (.rewind buffer)
    (are [x y] (= x y)
         100 (.getLong buffer)
         0 (.getInt buffer)
         1000 (.getInt buffer)
         1023 (.getInt buffer)
         0 (.getLong buffer)
         0 (.getLong buffer)
         :ready (enum-state (.getShort buffer))
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer)
         0 (.getInt buffer))))

