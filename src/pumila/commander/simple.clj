(ns pumila.commander.simple
  "A simple commander with no dependency, that runs
  tasks in your main thread pool.
  Primarily used for testing purpose."
  (:require [pumila.commander.protocol :as protocol]))

(defrecord SimpleCommander [counters timers]
  protocol/Commander
  (human-readable [_this] "")
  (execute! [_this task] (future (task)))
  (schedule! [this task _timeout-ms]
    (protocol/execute! this task))
  (mark! [_this metric event]
    (swap! counters update-in [metric event] (fnil inc 0)))
  (start-timer! [_this metric timer-name]
    (swap! timers assoc-in [metric timer-name] (System/nanoTime))
    [metric timer-name])
  (stop-timer! [_this timer-path]
    (let [now (System/nanoTime)
          timers (swap! timers update-in timer-path (fnil (fn [i] (- now i)) now))]
      (get-in timers timer-path))))

(defn new-commander [] (->SimpleCommander (atom {}) (atom {})))