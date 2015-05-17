(ns cleventing.event-sourcing-test
  (:require [clojure.test :refer :all]
            [cleventing.event-sourcing :refer :all]
            [clojure.java.io :as io]))

(defmethod accept :test
  [world event]
  :test-world)

(defmethod accept :replace
  [world event]
  :replaced-world)

(defn reset-event-store
  "Make sure there are no events to hydrate else the first test (hydration) will fail on the second run."
  [f]
  (let [current-label 0
        event-file (io/file "data" (str current-label ".events"))]
    (spit event-file ""))
  (f))

(use-fixtures :once reset-event-store)
(deftest raise!-test
  (testing "hydration"
    (is (= :test-world (hydrate "0"))))
  (testing "raising events"
    (let [event {:token 1}
          published (atom nil)
          on-event #(reset! published %)]
      (subscribe on-event)
      (raise! :test event)
      (is (= :test-world @world))
      (is (= (:token @published) 1))))
  (testing "rehydration"
    (let [replace-event {}])
    (raise! :replace {})
    (is (= :replaced-world (hydrate "0")))))

;TODO: this doesn't belong here
;(run-tests)

