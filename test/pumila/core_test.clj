(ns pumila.core-test
  (:require [clojure.test :refer :all]
            [pumila.core :as sut]
            [pumila.commander.reference :as commander]
            [pumila.commander.protocol :as protocol]
            [pumila.test-helpers :refer [laggy-fn]]))

(defmacro with-com
  [binding & body]
  `(let [~(first binding) (commander/make ~(second binding))]
     (try
       ~@body
       (finally
         (commander/close ~(first binding))))))

(deftest pumila-test
  (testing "The type of commander"
    (is (satisfies? protocol/Commander (with-com [com {}] com)))))

(deftest queue-test
  (testing "Queue the simplest way"
    (let [actual (with-com [com {}] (sut/queue com (laggy-fn 10)))
          actual-metas (meta actual)]
      (is (= true (deref actual 100 nil)))
      (is (= [nil nil]
             ((juxt :queue-duration :call-duration) actual-metas)))))

  (testing "Queue with metrics"
    (let [actual (with-com [com {}] (sut/queue com {:metric "mymet"} (laggy-fn 10)))
          {:keys [queue-duration call-duration]} (sut/unwrap-promises (meta actual) :deref-timeout 100)]
      (is (= true (deref actual 100 nil)))
      (is (> queue-duration 0) "Queuing the job took some time")
      (is (> call-duration 0) "Executing the job took some time")))

  (testing "Queue with timeout and error side-effect"
    (let [error-val (promise)
          actual (with-com [com {}]
                           (sut/queue com
                                      {:timeout 10
                                       :error-fn #(deliver error-val %)}
                                      (laggy-fn 10000)))]
      (is (= ::sut/timeout @actual))
      (is (instance? Exception @error-val))))

  (testing "Queue with timeout and error side-effect and fallback"
    (let [error-val (promise)
          fallback-val :fallback
          actual (with-com [com {}]
                           (sut/queue com
                                      {:timeout 10
                                       :error-fn #(deliver error-val %)
                                       :fallback-fn (constantly fallback-val)}
                                      (laggy-fn 10000)))]
      (is (= :fallback @actual))
      (is (instance? Exception @error-val)))))

(deftest exec-test
  (testing "Exec without metrics"
    (is (= true (with-com [com {}] (sut/exec com (laggy-fn 10)))))))