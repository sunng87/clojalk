(ns clojalk.jmx
  (:refer-clojure :exclude [use peek])
  (:use [clojalk data utils])
  (:require [clojure.contrib.jmx :as jmx])
  (:import [clojure.contrib.jmx Bean]))

(defn new-mbean [state-ref]
  (proxy [Bean] [state-ref]
    (getAttribute [attr] 
       (let [attr-value (@(.state ^clojure.contrib.jmx.Bean this) (keyword attr))]
         (if (fn? attr-value)
           (attr-value)
           attr-value)))))

(defn- workers []
  (map #(name (:id @%)) (filter #(= :worker (:type @%)) (vals @sessions))))

(defn- producers []
  (map #(name (:id @%)) (filter #(= :producer (:type @%)) (vals @sessions))))

(def jmx-session-bean
  (new-mbean 
    (ref 
      {:workers (fn [] (into-string-array (workers)))
       :producers (fn [] (into-string-array (producers)))})))

(def jmx-job-bean
  (new-mbean
    (ref
      {:total-jobs (fn [] (count @jobs))
       })))

(def jmx-tube-bean
  (new-mbean
    (ref
      {:tubes (fn [] (into-string-array (map #(name (:name @%)) (vals @tubes))))
       })))
  
(defn start-jmx-server []
  (jmx/register-mbean jmx-session-bean "clojalk.management:type=Sessions")
  (jmx/register-mbean jmx-job-bean "clojalk.management:type=Jobs")
  (jmx/register-mbean jmx-tube-bean "clojalk.management:type=Tubes"))
