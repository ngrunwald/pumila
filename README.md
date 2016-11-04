# pumila

Lighter Clojure replacement for Netflix hystrix latency and fault tolerance library

## Usage

```clojure
(defn get-my-uri
  [uri]
  (:body (http/get uri)))

(def commander (pm/make-commander {:label "http-client" :max-size 5}))

(let [;; async
      f (pm/queue commander {:timeout 1000 :error-fn println :fallback-fn (constantly "I feel lucky!")} (get-my-uri "http://google.com"))
      result1 @f

      ;; sync
      result2 (pm/exec commander {:timeout 1000 :error-fn println :fallback-fn (constantly "I feel lucky!")} (get-my-uri "http://google.com"))])
```

## License

Copyright © 2016 Oscaro.com

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
