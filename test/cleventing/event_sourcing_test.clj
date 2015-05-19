(ns cleventing.event-sourcing-test
  (:require [clojure.test :refer :all]
            [cleventing.event-sourcing :refer :all]
            [clojure.java.io :as io]))

(def test-aggregate-id :testing)

(defmethod accept :test
  [_ _]
  :test-world)

(defmethod accept :replace
  [_ _]
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
    (is (= :test-world (test-aggregate-id (hydrate "0")))))
  (testing "raising events"
    (let [event {:token 1}
          published (atom nil)
          on-event #(reset! published %)]
      (subscribe on-event)
      (raise! :test event test-aggregate-id)
      (is (= :test-world (get-aggregate test-aggregate-id)))
      (is (= (:token @published) 1))))
  (testing "rehydration"
    (raise! :replace {} test-aggregate-id)
    (is (= :replaced-world (test-aggregate-id (hydrate "0"))))))


