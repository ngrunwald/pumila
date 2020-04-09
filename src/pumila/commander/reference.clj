(ns pumila.commander.reference
  "A reference implementation of a commander based on clojure-metrics
   and java executor service."
  (:require [metrics.meters :as met]
            [metrics.timers :as tmr]
            [pumila.commander.protocol :as protocol])
  (:import (java.util.concurrent
            ExecutorService
            TimeUnit
            ScheduledThreadPoolExecutor
            ScheduledExecutorService)
           (io.aleph.dirigiste Executor Executors Stats$Metric)
           (java.util EnumSet)))

(defn metric-name
  [metric nam]
  (if (string? metric)
    (str metric "-" nam)
    (conj (into [] (take 2 metric)) nam)))

(defn mk-meter
  [registry metric m-name]
  (if registry
    (met/meter registry (metric-name metric m-name))
    (met/meter (metric-name metric m-name))))

(defn start-timer
  [registry metric m-name]
  (tmr/start
   (if registry
     (tmr/timer registry (metric-name metric m-name))
     (tmr/timer (metric-name metric m-name)))))

(defrecord Commander
  [label
   options
   ^ExecutorService executor
   ^ScheduledExecutorService scheduler
   registry
   active]
  protocol/Commander
  (human-readable [_this] label)
  (execute! [_this task] (.submit executor ^Runnable task))
  (schedule! [_this task timeout-ms]
    (.schedule ^ScheduledExecutorService scheduler ^Runnable task ^long timeout-ms TimeUnit/MILLISECONDS))
  (mark! [_this metric event] (met/mark! (mk-meter registry metric event)))
  (start-timer! [_this metric timer-name] (start-timer registry metric timer-name))
  (stop-timer! [_this timer] (tmr/stop timer)))

(defn make
  ([{:keys [label registry] :as options} executor]
   (let [scheduler (ScheduledThreadPoolExecutor. 0)]
     (.setMaximumPoolSize scheduler 1)
     (.setRemoveOnCancelPolicy scheduler true)
     (map->Commander {:label label :options options
                      :executor executor :scheduler scheduler
                      :registry registry
                      :active (atom 0)})))
  ([{:keys [utilization max-size all-stats]
     :or {utilization 0.8 max-size 10}
     :as opts}]
   (if all-stats
     (make opts (Executors/utilizationExecutor utilization (int max-size)))
     (make opts (Executors/utilizationExecutor utilization (int max-size) (EnumSet/allOf Stats$Metric)))))
  ([] (make {})))

(defn close
  [{:keys [executor scheduler] :as commander}]
  (.shutdown ^ScheduledExecutorService scheduler)
  (.shutdown ^Executor executor)
  commander)

(defn await-termination
  ([commander]
   (await-termination commander 60000))
  ([{:keys [^Executor executor] :as commander} termination-delay-ms]
   (close commander)
   (.awaitTermination executor termination-delay-ms TimeUnit/MILLISECONDS)))