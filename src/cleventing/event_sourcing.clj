(ns cleventing.event-sourcing
  "Event sourcing http://martinfowler.com/eaaDev/EventSourcing.html"
  (:require [clojure.java.io :refer :all]
            [clojure.pprint]
            [clojure.edn]))


(defmulti accept
  "Signature for accepting an event, applying it to world.
  This will be implemented by your command/accept pairs.
  Commands check the validity, and raise events.
  accept performs a data transform, updating the aggregate,
  which is called from raise, or when hydrating an event stream."
  (fn [event aggregate] (:event event)))

(def ^:private domain
  "The domain, the state of everything."
  (atom {}))

(defn get-aggregate
  "Get the aggregate with the given ID"
  [id]
  (get @domain id))

(let [eof (Object.)]
  (defn- read-all
    "A sequence of reading clojure data from a file."
    [^java.io.PushbackReader reader]
    (take-while #(not= eof %)
                (repeatedly #(clojure.edn/read {:eof eof} reader)))))

(defn- data-reader
  "Constructs a PushbackReader for a data file"
  [& more]
  (java.io.PushbackReader.
   (reader (file "data" (apply str more)))))

(let [current-label (atom nil)
      event-count (atom 0)
      subscriptions (atom #{})
      events-per-snapshot 20]

  (defn snapshot
    "Switches to a new label and writes out the current domain state asynchronously."
    [domain]
    (let [sf (java.text.SimpleDateFormat.
              "yyyy_MM_dd__HH_mm_ss__SSS")
          now (java.util.Date.)
          label (.format sf now)
          state-file (file "data" (str label ".state"))]
      (reset! current-label label)
      (reset! event-count 0)
      ; write the snapshot asynchronously on another thread to avoid delays
      ; the domain we are looking at is immutable
      (future
        (io! (with-open [w (writer state-file)]
               (clojure.pprint/pprint domain w))))))

  (defn- store
    "Writes an event to file"
    [event]
    (let [event-file (file "data" (str @current-label ".events"))]
      (io! (with-open [w (writer event-file :append true)]
             (clojure.pprint/pprint event w)))
      (when (>= (event :seq) events-per-snapshot)
        (snapshot @domain))))

  (defn hydrate
    "Load domain state and process the event log.
    Specify the event-id for a particular point in time,
    or event-id 0 if you do not want any events applied."
    ([label] (hydrate label nil))
    ([label event-id]
     (reset! current-label label)
     (io! (with-open [state-reader (data-reader label ".state")
                      event-reader (data-reader label ".events")]
            (let [snapshot (clojure.edn/read state-reader)
                  all-events (read-all event-reader)
                  events (if event-id
                           (take-while #(<= (% :seq) event-id) all-events)
                           all-events)]
              (reset! event-count (or (:seq (last events)) 0))
              (reduce #(assoc %1 (:aggregate-id %2) (accept %2 (get-aggregate (:aggregate-id %2)))) snapshot events))))))

  (defn hydrate-latest
    "Fully hydrate the most recent domain state snapshot and event log."
    []
    (let [get-name #(.getName %)
          file-name (-> "data" file file-seq sort reverse first get-name)
          label (.replace file-name ".state" "")]
      (hydrate label)))

  (defn bootstrap
    "Bring domain up to latest snapshot + events"
    []
    (reset! domain (hydrate-latest)))

  (defn subscribe
    "Subscribe function f be called on every event raised."
    [f]
    (swap! subscriptions conj f))

  (defn unsubscribe [f]
    (swap! subscriptions disj f))

  (defn- publish [event]
    "Publishing allows for external read models to be denormalized.
    You can process these events on an external server that handles just reads.
    http://martinfowler.com/bliki/CQRS.html"
    (doseq [f @subscriptions]
      (f event)))

  ; the symantics I want are:
  ; may be called from multiple threads, needs to be done in serial, want a return value to indicate success
  (let [o (Object.)]
    (defn raise!
      "Raising an event stores it, publishes it, and updates the aggregate model."
      [event-type event aggregate-id ]
      (assert @current-label "Must call hydrate or snapshot prior to recording events")
      (locking o
        (let [event (assoc event
                      :event event-type
                      :aggregate-id aggregate-id
                      :when (java.util.Date.)
                      :seq (swap! event-count inc))]
          (store event)
          (publish event)
          (swap! domain assoc aggregate-id (accept event (get-aggregate aggregate-id))))))))

