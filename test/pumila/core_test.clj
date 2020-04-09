(ns pumila.core-test
  (:require [expectations :refer :all]
            [pumila.core :refer :all]))

(defn laggy-fn
  [lag]
  (Thread/sleep lag)
  true)

(defmacro with-com
  [binding & body]
  `(let [~(first binding) (make-commander ~(second binding))]
     (try
       ~@body
       (finally
         (close ~(first binding))))))

(expect pumila.core.Commander (with-com [com {}] com))
(expect (more-of p
                 true (deref p 100 nil)
                 nil (:queue-duration (meta p))
                 nil (:call-duration (meta p)))
        (with-com [com {}]
          (queue com (laggy-fn 10))))
(expect true (with-com [com  {}]
               (exec com (laggy-fn 10))))
(expect (more-of p
                 true (deref p 100 nil)
                 #(>= % 0) (deref (:queue-duration (meta p)) 100 nil)
                 #(>= % 10) (deref (:call-duration (meta p)) 100 nil))
        (with-com [com {}]
          (queue com {:metric "mymet"} (laggy-fn 10))))
(expect (more-of [p tmout]
                 :pumila.core/timeout (deref p)
                 Exception (deref tmout))
        (with-com [com {}]
          (let [tmout (promise)]
            [(queue com {:timeout 10 :error-fn #(deliver tmout %)} (laggy-fn 10000)) tmout])))
(expect (more-of [p tmout]
                 :fallback (deref p 100 nil)
                 Exception (deref tmout 100 nil))
        (with-com [com {}]
          (let [tmout (promise)]
            [(queue com {:error-fn #(deliver tmout %) :fallback-fn (constantly :fallback)}
                    ((fn [] (throw (ex-info "error" {})))))
             tmout])))
