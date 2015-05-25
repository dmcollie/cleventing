(ns event-sourcing.core
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

(defmethod accept :default
  [event aggregate]
  (throw (ex-info "Unhandled event" {:event event :aggregate aggregate})))

(defonce ^:private state (atom {}))

(defn create-new-uuid [] (java.util.UUID/randomUUID))

(defn dump-state
  "Dump the state to allow it to be inspected"
  []
  @state)

(defmulti index-aggregate
  "Return a set of indicies relevant to the given aggregate.
  Allows, for example, searching by name."
  (fn [aggregate] (:aggregate-type aggregate)))

(defmethod index-aggregate :default
  [_]
  {})

(defn create-aggregate
  "Create a new aggregate of type t with map value v"
  [t v]
  (let [aggregate-id (create-new-uuid)]
    (swap! state assoc aggregate-id (merge {:aggregate-type t} v))
    aggregate-id))

(defn delete-aggregate
  "Delete the aggregate with the given id"
  [id]
  (swap! state dissoc id))

(defn get-aggregate
  "Get the aggregate with the given ID"
  [id]
  (get @state id))

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

  (defn- store-event
    "Store the event to file, can take nil to init the event file"
    ([] (store-event nil))
    ([event]
     (let [event-file (file "data" (str @current-label ".events"))]
       (io! (with-open [w (writer event-file :append true)]
              (when event
                (clojure.pprint/pprint event w)))))))

  (defn snapshot
    "Switches to a new label and writes out the current domain state asynchronously."
    []
    (let [sf (java.text.SimpleDateFormat.
              "yyyy_MM_dd__HH_mm_ss__SSS")
          now (java.util.Date.)
          label (.format sf now)
          state-file (file "data" (str label ".state"))]
      (reset! current-label label)
      (reset! event-count 0)
      ; write the snapshot asynchronously on another thread to avoid delays
      ; the domain we are looking at is immutable. Also write out a blank event storage
      ; file
      (future
        (do
          (io! (with-open [w (writer state-file)]
                 (clojure.pprint/pprint @state w)))
          (store-event)))))

  (defn- store
    "Writes an event to file"
    [event]
    (do
      (store-event event)
      (when (>= (event :seq) events-per-snapshot)
        (snapshot))))

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
    "Fully hydrate the most recent domain state snapshot and event log. If there is no snapshot then make one."
    []
    (let [include-state-only (fn [files] (filter #(.endsWith (.getName %) ".state") files))
          latest-state-file (-> "data" file file-seq include-state-only sort reverse first)]
      (if latest-state-file
        (let [state-filename (.getName latest-state-file)
              label (.replace state-filename ".state" "")]
          (hydrate label))
        (snapshot))))

  (defn bootstrap
    "Bring domain up to latest snapshot + events"
    []
    (reset! state (hydrate-latest)))

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
          (swap! state assoc aggregate-id (accept event (get-aggregate aggregate-id))))))))

