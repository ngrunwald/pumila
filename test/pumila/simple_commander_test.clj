(ns pumila.simple-commander-test
  (:require [clojure.test :refer :all]
            [pumila.test-helpers :refer [laggy-fn]]
            [pumila.commander.simple :as sut]
            [pumila.core :as pumila]))

(deftest simple-commander-test
  (testing "Queue the simplest way"
    (let [commander (sut/new-commander)
          actual (pumila/queue commander (laggy-fn 10))
          actual-metas (meta actual)]
      (is (= true (deref actual 100 nil)))
      (is (= [nil nil]
             ((juxt :queue-duration :call-duration) actual-metas)))))

  (testing "Queue with metrics"
    (let [commander (sut/new-commander)
          actual (pumila/queue commander {:metric "mymet"} (laggy-fn 10))
          {:keys [queue-duration call-duration]} (pumila/unwrap-promises (meta actual) :deref-timeout 100)]
      (is (= {"mymet" {"success" 1}} (deref (:counters commander))))
      (let [my-met-timers (get (deref (:timers commander)) "mymet")]
        (is (> (get my-met-timers "call-duration") 10000000))
        (is (> (get my-met-timers "queue-duration") 0)))
      (is (= true (deref actual 100 nil)))
      (is (> queue-duration 0) "Queuing the job took some time")
      (is (> call-duration 10) "Executing the job took some time")))

  (testing "Queue with timeout and error side-effect"
    (let [commander (sut/new-commander)
          error-val (promise)
          actual (pumila/queue commander
                            {:timeout 10
                             :error-fn #(deliver error-val %)}
                            (laggy-fn 10000))]
      (is (= ::pumila/timeout @actual))
      (is (instance? Exception @error-val))))

  (testing "Queue with timeout and error side-effect and fallback"
    (let [commander (sut/new-commander)
          error-val (promise)
          fallback-val :fallback
          actual (pumila/queue commander
                            {:timeout 10
                             :error-fn #(deliver error-val %)
                             :fallback-fn (constantly fallback-val)}
                            (laggy-fn 10000))]
      (is (= :fallback @actual))
      (is (instance? Exception @error-val)))))
