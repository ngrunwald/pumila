(defproject pumila "0.1.2"
  :description "Lighter replacement for Netflix hystrix latency and fault tolerance library"
  :url "https://github.com/ngrunwald/pumila"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [io.aleph/dirigiste "0.1.2"]
                 [metrics-clojure "2.6.1"]]
  :profiles {:dev {:dependencies  [[expectations "2.1.8"]]
                   :plugins [[lein-expectations "0.0.7"]]}})
