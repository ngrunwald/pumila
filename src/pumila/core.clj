(ns pumila.core
  (:import [java.util.concurrent Future]
           [clojure.lang ExceptionInfo IMeta]))


(defprotocol Commander
  (human-readable [this] "a human readable string")
  (execute! [this ^Runnable  task] "Executes the task. Must return a deferencable object (like a future or a promise).")
  (schedule! [this ^Runnable  task timeout-ms] "Executes task in the within timeout (in ms). Must return a deferencable object (like a future or a promise).")
  (mark! [this metric event] "mark when an event happens. metric gives context to the event.")
  (start-timer! [this metric timer-name] "starts a timer and returns it")
  (stop-timer! [this timer] "stops a timer"))


(defn add-meta
  [v m]
  (if (and m (instance? IMeta v))
    (with-meta v m)
    v))

(defn ms
  [v]
  (when v (/ v 1000000.0)))

(defn submit-task
  [commander task]
  (let [started (System/currentTimeMillis)
        fut (execute! commander task)]
    [fut (- (System/currentTimeMillis) started)]))



(defn mk-task
  ^Runnable
  [commander options promises run-fn args]
  (let [{:keys [timeout fallback-fn error-fn metric]} options
        {:keys [result queue-duration-atom
                call-duration-atom cancel-future-p]} promises
        queue-timer (when metric (start-timer! commander metric "queue-duration"))]
    (fn [] (let [res (let [queue-latency (when metric (stop-timer! commander queue-timer))
                           call-timer (when metric (start-timer! commander metric "call-duration"))]
                       (when metric
                         (deliver queue-duration-atom (ms queue-latency)))
                       (try
                         (let [call-result (apply run-fn args)
                               latency (when call-timer (stop-timer! commander call-timer))]
                           (when metric
                             (deliver call-duration-atom (ms latency))
                             (mark! commander metric "success"))
                           (when timeout
                             (when-let [^Future cancel-fut (deref cancel-future-p 1 nil)]
                               (.cancel cancel-fut true)))
                           call-result)
                         (catch InterruptedException _
                           ::skip)
                         (catch Exception e
                           (let [exi (ex-info "Error calling command"
                                              {:commander (human-readable commander)
                                               :args args
                                               :type :error
                                               :options options}
                                              e)]
                             (when-let [exp (::exp options)] (deliver exp exi))
                             (when metric
                               (deliver queue-duration-atom (ms queue-latency))
                               (mark! commander metric "failure"))
                             (when error-fn
                               (try (error-fn exi)
                                    (catch Exception _ nil)))
                             (when fallback-fn
                               (try
                                 (let [fallback-res (apply fallback-fn args)
                                       latency (when call-timer (stop-timer! commander call-timer))]
                                   (when metric
                                     (deliver call-duration-atom (ms latency)))
                                   fallback-res)
                                 (catch Exception _ nil)))))))]
             (when-not (= ::skip res)
               (deliver result res))))))

(defn handle-error
  ([error-fn e msg context]
   (try
     (error-fn (if (instance? ExceptionInfo e)
                 e
                 (ex-info msg context e)))
     (catch Exception _ nil)))
  ([error-fn e msg]
   (handle-error error-fn e msg (ex-data e)))
  ([error-fn ^Exception e]
   (handle-error error-fn  e (.getMessage e) (ex-data e))))

(defn mk-timeout-task
  ^Runnable
  [commander options result ^Future fut args]
  (let [{:keys [timeout fallback-fn error-fn timeout-val metric]
         :or {timeout-val ::timeout}} options]
    (fn []
      (try
        (when (not (realized? result))
          (.cancel fut true)
          (when metric
            (mark! commander metric "failure"))
          (when error-fn
            (let [e (ex-info "Timeout with queue asynchronous call"
                             {:commander (human-readable commander)
                              :args args
                              :timeout timeout
                              :type :timeout
                              :options options})]
              (handle-error error-fn e)))
          (if fallback-fn
            (try
              (deliver result (apply fallback-fn args))
              (catch Exception _
                (deliver result timeout-val)))
            (deliver result timeout-val)))
        (catch Exception e
          (when error-fn
            (handle-error error-fn e "Pumila Error")))))))

(defn queue*
  [commander options run-fn args]
  (let [{:keys [metric timeout]} options
        result (promise)
        queue-duration-atom (when metric (promise))
        call-duration-atom (when metric (promise))
        cancel-future-p (when timeout (promise))
        promises {:result result
                  :queue-duration-atom queue-duration-atom
                  :call-duration-atom call-duration-atom
                  :cancel-future-p cancel-future-p}
        ^Runnable task (mk-task commander options promises run-fn args)
        [fut elapsed] (submit-task commander task)
        ^Runnable timeout-task (when timeout
                                 (mk-timeout-task commander options result fut args))]
    (when timeout-task
      (let [cancel-fut (schedule! commander timeout-task (- timeout elapsed))]
        (deliver cancel-future-p cancel-fut)))
    (if metric
      (add-meta result {:queue-duration queue-duration-atom :call-duration call-duration-atom})
      result)))

(defn exec*
  [commander
   {:keys [timeout fallback-fn error-fn] :as options}
   run-fn args]
  (let [exp (promise)
        opts (-> options
                 (assoc ::exp exp)
                 (dissoc :timeout))
        p (queue* commander opts run-fn args)
        result (if timeout
                 (deref p timeout ::timeout)
                 (deref p))
        queue-duration (some-> p (meta) (:queue-duration) (deref 1 nil))
        call-duration (some-> p (meta) (:call-duration) (deref 1 nil))]
    (cond
      (realized? exp) (throw @exp)
      (= result ::timeout) (let [e (ex-info "Timeout with exec synchronous call"
                                            {:commander (:label commander) :args args
                                             :timeout timeout :type :timeout :options options})]
                             (when error-fn (error-fn e))
                             (if fallback-fn
                               (add-meta
                                (apply fallback-fn args)
                                {:queue-duration queue-duration})
                               (throw e)))
      :else (add-meta result
                      {:queue-duration queue-duration
                       :call-duration call-duration}))))

(defmacro queue
  ([commander options [run-fn & args]]
   `(queue* ~commander ~options ~run-fn (vector ~@args)))
  ([commander body]
   `(queue ~commander {} ~body)))

(defmacro exec
  ([commander options [run-fn & args]]
   `(exec* ~commander ~options ~run-fn (vector ~@args)))
  ([commander body]
   `(exec ~commander {} ~body)))

(defn unwrap-promises
  "The queue function will return metrics as meta-data
  in the form of promise. Calling this function will
  deref each promises and return a map of their corresponding
  values"
  [m & {:keys [deref-timeout]
        :or {deref-timeout 1}}]
  (persistent!
   (reduce (fn [acc [k v]]
             (if (instance? clojure.lang.IPending v)
               (assoc! acc k (deref v deref-timeout nil))
               acc))
           (transient m) m)))
