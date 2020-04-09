(ns pumila.core
  (:require [metrics
             [timers :as tmr]
             [meters :as met]]
            [diehard.core :as diehard])
  (:import [io.aleph.dirigiste Executor Executors Stats$Metric]
           [java.util.concurrent ExecutorService Future
            ScheduledExecutorService ScheduledThreadPoolExecutor]
           [java.util EnumSet]
           [java.util.concurrent TimeUnit]
           [clojure.lang ExceptionInfo IMeta]))

(defrecord Commander [label options executor scheduler registry active])

(defn make-commander
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
     (make-commander opts (Executors/utilizationExecutor utilization (int max-size)))
     (make-commander opts (Executors/utilizationExecutor utilization (int max-size) (EnumSet/allOf Stats$Metric)))))
  ([] (make-commander {})))

(defn close
  [{:keys [executor scheduler] :as commander}]
  (.shutdown scheduler)
  (.shutdown executor)
  commander)

(defn await-termination
  ([commander]
   (await-termination commander 60000))
  ([{:keys [^Executor executor] :as commander} termination-delay-ms]
   (close commander)
   (.awaitTermination executor termination-delay-ms TimeUnit/MILLISECONDS)))

(defn add-meta
  [v m]
  (if (and m (instance? IMeta v))
    (with-meta v m)
    v))

(defn ms
  [v]
  (when v (/ v 1000000.0)))

(defn metric-name
  [metric nam]
  (if (string? metric)
    (str metric "-" nam)
    (conj (into [] (take 2 metric)) nam)))

(defn submit
  [executor task]
  (.submit executor task))

(def retry-policies {:none    {:max-retries 0}
                     :block   {:delay-ms 100}
                     :backoff {:backoff-ms [100 120000 4]}})

(defn mk-retry-policy
  [opts]
  (if (map? opts)
    opts
    (get retry-policies opts {:max-retries 0})))

(defn try-until-free
  ([executor task {:keys [reject-policy timeout]}]
   (let [retry-policy (mk-retry-policy reject-policy)
         retry-policy (if timeout
                        (assoc retry-policy :max-duration-ms timeout)
                        retry-policy)
         started (System/currentTimeMillis)
         fut (diehard/with-retry retry-policy
               (submit executor task))]
     [fut (- (System/currentTimeMillis) started)]))
  ([executor task] (try-until-free executor task {})))

(defn start-timer
  [registry metric m-name]
  (tmr/start
   (if registry
     (tmr/timer registry (metric-name metric m-name))
     (tmr/timer (metric-name metric m-name)))))

(defn mk-meter
  [registry metric m-name]
  (if registry
    (met/meter registry (metric-name metric m-name))
    (met/meter (metric-name metric m-name))))

(defn mk-task
  ^Runnable
  [commander options promises run-fn args]
  (let [registry (or (:registry options) (:registry commander))
        {:keys [timeout fallback-fn error-fn metric]} options
        {:keys [result queue-duration-atom
                call-duration-atom cancel-future-p]} promises
        queue-timer (when metric (start-timer registry metric "queue-duration"))]
    (fn [] (let [res (let [queue-latency (when metric (tmr/stop queue-timer))
                           call-timer (when metric (start-timer registry metric "call-duration"))]
                       (when metric
                         (deliver queue-duration-atom (ms queue-latency)))
                       (try
                         (let [call-result (apply run-fn args)
                               latency (when call-timer (tmr/stop call-timer))]
                           (when metric
                             (deliver call-duration-atom (ms latency))
                             (met/mark! (mk-meter registry metric "success")))
                           (when timeout
                             (when-let [^Future cancel-fut (deref cancel-future-p 1 nil)]
                               (.cancel cancel-fut true)))
                           call-result)
                         (catch InterruptedException _
                           ::skip)
                         (catch Exception e
                           (let [exi (ex-info "Error calling command"
                                              {:commander (:label commander) :args args
                                               :type :error :options options} e)]
                             (when-let [exp (::exp options)] (deliver exp exi))
                             (when metric
                               (deliver queue-duration-atom (ms queue-latency))
                               (met/mark! (mk-meter registry metric "failure")))
                             (when error-fn
                               (try (error-fn exi)
                                    (catch Exception _ nil)))
                             (when fallback-fn
                               (try
                                 (let [fallback-res (apply fallback-fn args)
                                       latency (when call-timer (tmr/stop call-timer))]
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
  ([error-fn e]
   (handle-error error-fn e (.getMessage e) (ex-data e))))

(defn mk-timeout-task
  ^Runnable
  [commander options result fut args]
  (let [registry (or (:registry options) (:registry commander))
        {:keys [timeout fallback-fn error-fn timeout-val metric]
         :or {timeout-val ::timeout}} options]
    (fn []
      (try
        (when (not (realized? result))
          (.cancel fut true)
          (when metric
            (met/mark! (mk-meter registry metric "failure")))
          (when error-fn
            (let [e (ex-info "Timeout with queue asynchronous call"
                             {:commander (:label commander) :args args
                              :timeout timeout :type :timeout
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
  (let [^ExecutorService executor (:executor commander)
        ^ScheduledExecutorService scheduler (:scheduler commander)
        {:keys [metric timeout reject-policy]} options
        result (promise)
        queue-duration-atom (when metric (promise))
        call-duration-atom (when metric (promise))
        cancel-future-p (when timeout (promise))
        promises {:result result
                  :queue-duration-atom queue-duration-atom
                  :call-duration-atom call-duration-atom
                  :cancel-future-p cancel-future-p}
        ^Runnable task (mk-task commander options promises run-fn args)
        [fut elapsed] (try-until-free executor task {:reject-policy reject-policy :timeout timeout})
        ^Runnable timeout-task (when timeout
                                 (mk-timeout-task commander options result fut args))]
    (when timeout-task
      (let [cancel-fut (.schedule scheduler timeout-task
                                  (- timeout elapsed) TimeUnit/MILLISECONDS)]
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
