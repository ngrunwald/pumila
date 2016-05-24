(ns pumila.core-test
  (:require [expectations :refer :all]
            [pumila.core :refer :all]))

(defn laggy-fn
  [lag]
  (Thread/sleep lag)
  true)

(let [com (make-commander)]
  (try
    (expect pumila.core.Commander com)
    (expect (more-of p
                     true (deref p 100 nil)
                     nil (:queue-duration (meta p))
                     nil (:call-duration (meta p)))
            (queue com (laggy-fn 10)))
    (expect (more-of p
                     true (deref p 100 nil)
                     #(>= % 0) (deref (:queue-duration (meta p)) 100 nil)
                     #(>= % 10) (deref (:call-duration (meta p)) 100 nil))
            (queue com {:metric "mymet"} (laggy-fn 10)))
    (finally
      (close com))))
