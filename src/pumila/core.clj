(ns pumila.core
  (:require [pumila.impl :as impl]))

(defmacro queue
  ([commander options [run-fn & args]]
   `(impl/queue* ~commander ~options ~run-fn (vector ~@args)))
  ([commander body]
   `(queue ~commander {} ~body)))

(defmacro exec
  ([commander options [run-fn & args]]
   `(impl/exec* ~commander ~options ~run-fn (vector ~@args)))
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
