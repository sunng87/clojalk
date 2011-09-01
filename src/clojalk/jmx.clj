(ns clojalk.jmx
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core utils])
  (:require [clojure.contrib.jmx :as jmx])
  (:import [clojure.contrib.jmx Bean]))

(defn new-mbean [state-ref]
  (proxy [Bean] [state-ref]
    (getAttribute [attr] 
       (let [attr-value (@(.state this) (keyword attr))]
         (if (fn? attr-value)
           (attr-value)
           attr-value)))))

(def jmx-bean
  (new-mbean 
    (ref 
      {:version "1.0.0-alpha"
       :tubes (fn [] (into-string-array (map #(name (:name @%)) (vals @tubes))))
       :sessions (fn [] (into-string-array (map #(:id @%) (vals @sessions))))})))
  
(defn start-jmx-server []
  (jmx/register-mbean jmx-bean "clojalk.management:type=Monitor"))
