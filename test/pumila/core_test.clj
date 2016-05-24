(ns pumila.core-test
  (:require [expectations :refer :all]
            [pumila.core :refer :all]
            [metrics
             [meters :as met]
             [timers :as tmr]]))

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
    (expect true (exec com (laggy-fn 10)))
    (expect (more-of p
                     true (deref p 100 nil)
                     #(>= % 0) (deref (:queue-duration (meta p)) 100 nil)
                     #(>= % 10) (deref (:call-duration (meta p)) 100 nil))
            (queue com {:metric "mymet"} (laggy-fn 10)))
    (let [tmout (promise)]
      (expect (more-of p
                       ;; :pumila.core/timeout (deref p)
                       Exception (deref tmout))
              (queue com {:timeout 10 :error-fn #(deliver tmout %)} (laggy-fn 1000))))
    (let [tmout (promise)]
      (expect (more-of p
                       :fallback (deref p 100 nil)
                       Exception (deref tmout 100 nil))
              (queue com {:error-fn #(deliver tmout %) :fallback-fn (constantly :fallback)}
                     ((fn [] (throw (ex-info "error" {})))))))))
