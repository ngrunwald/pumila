(defproject pumila "0.1.6-SNAPSHOT"
  :description "Lighter replacement for Netflix hystrix latency and fault tolerance library"
  :url "https://github.com/ngrunwald/pumila"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [io.aleph/dirigiste "0.1.5"]
                 [diehard "0.9.4"]
                 [metrics-clojure "2.10.0"]]
  :profiles {:dev {:dependencies  [[expectations "2.1.10"]]
                   :plugins [[lein-expectations "0.0.7"]]}}
  :global-vars  {*warn-on-reflection* true}
  :repl-options {:init-ns pumila.core})
