(ns pumila.commander.protocol)

(defprotocol Commander
  (human-readable [this] "a human readable string")
  (execute! [this ^Runnable task] "Executes the task. Must return a deferencable object (like a future or a promise).")
  (schedule! [this ^Runnable task timeout-ms] "Executes task in the within timeout (in ms). Must return a deferencable object (like a future or a promise).")
  (mark! [this metric event] "mark when an event happens. metric gives context to the event.")
  (start-timer! [this metric timer-name] "starts a timer and returns it")
  (stop-timer! [this timer] "stops a timer. Must return the number of nanos since it started."))