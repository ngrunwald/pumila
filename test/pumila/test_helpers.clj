(ns pumila.test-helpers
  (:require [clojure.test :refer :all]))

(defn laggy-fn
  [lag]
  (Thread/sleep lag)
  true)