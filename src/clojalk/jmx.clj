(ns clojalk.jmx
  (:refer-clojure :exclude [use peek])
  (:use [clojalk core utils])
  (:use [clojure.string :only [join]])
  (:require [clojure.contrib.jmx :as jmx])
  (:import [clojure.contrib.jmx Bean]))

(def jmx-bean
  (Bean. 
    (ref 
      {:version "1.0.0-alpha"
       :tubes (join "," (map #(:name @%) (vals @tubes)))
       :sessions (join "," (map #(:id @%) (vals @sessions)))})))
  
(defn start-jmx-server []
  (jmx/register-mbean jmx-bean "clojalk.management:type=Monitor"))
