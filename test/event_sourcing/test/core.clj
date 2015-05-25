(ns event-sourcing.test.core
  (:require [clojure.test :refer :all]
            [event-sourcing.core :refer :all]
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
    (is (= :test-world (:value (test-aggregate-id (hydrate "0"))))))
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

(deftest unhandled-events-test
  (testing "Unhandled events"
    (is (thrown? Exception (accept :the-cake-is-a-lie {} {})))))

(deftest aggregates-test
  (testing "create aggregate"
    (let [aggregate-id (create-aggregate :patient {:name "Dave Collie" :age 47 :gender :male})]
      (is (= java.util.UUID (type aggregate-id)))
      (is (= {:name "Dave Collie" :age 47 :gender :male} (get-aggregate aggregate-id)))))
  (testing "delete aggregate"
    (let [initial-state (dump-state)
          aggregate-id (create-aggregate :patient {:name "Dave Collie" :age 47 :gender :male})]
      (is (= initial-state (delete-aggregate aggregate-id))))))



