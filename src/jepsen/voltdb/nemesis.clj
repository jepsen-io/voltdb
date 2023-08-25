(ns jepsen.voltdb.nemesis
  "Various packages of faults we can inject against VoltDB clusters."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
             [nemesis :as n]
             [generator :as gen]
             [net :as net]
             [util :as util]]
            [jepsen.control [util :as cu]]
            [jepsen.nemesis [combined :as nc]
             [time :as nt]]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [client :as vc]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.voltdb.client ClientResponse)))


(defn rando-nemesis
  "Tries to confuse VoltDB by spewing random writes into an unrelated table.
  This happens in an independent thread; the nemesis returns from :invoke
  immediately, even though it's still writing.

  Expects invocations of the form {:f :rando, :value some-node}."
  [opts]
  (let [initialized? (promise)
        writes       (atom 0)]
    (reify n/Nemesis
      (setup! [this test]
        (let [node (first (:nodes test))
              conn (vc/connect node {})]
          (when (deliver initialized? true)
            (try
              (c/on node
                    (vc/with-race-retry
                      ; Create table
                      (voltdb/sql-cmd! "CREATE TABLE mentions (
                                       well     INTEGER NOT NULL,
                                       actually INTEGER NOT NULL
                                       );"))
                    (info node "mentions table created"))
              (finally
                (vc/close! conn)))))
        this)

      (invoke! [this test op]
        (assert (= :rando (:f op)))
        (let [conn (vc/connect (:value op)
                               {:procedure-call-timeout 100
                                :reconnect? false})]
          (future
            (util/with-thread-name "rando"
              (try
                (let [; Run for 10 seconds
                      deadline (+ (System/nanoTime)
                                  (* 10 1e9))]
                  (loop [i 0]
                    (when (< (System/nanoTime) deadline)
                      ; (call! conn "MENTIONS.insert" i (rand-int 1000)))
                      ; If we go TOO fast we'll start forcing other ops to
                      ; time out. If we go too slow we won't get a long
                      ; enough log.
                      (Thread/sleep 1)
                      (vc/async-call!
                        conn "MENTIONS.insert" i (rand-int 1000)
                        (fn [^ClientResponse res]
                          (when (or (= ClientResponse/SUCCESS
                                       (.getStatus res))
                                    ; not sure why this happens but it's ok?
                                    (= ClientResponse/UNINITIALIZED_APP_STATUS_CODE
                                       (.getStatus res)))
                            (->> res
                                 .getResults
                                 (map vc/volt-table->map)
                                 first
                                 :rows
                                 first
                                 :modified_tuples
                                 (swap! writes +)))))
                      (recur (inc i)))))
                (catch Exception e
                  (info "Rando nemesis crashed with" (.getMessage e)))
                (finally
                  (info "Rando nemesis finished writing")
                  (vc/close! conn))))))
        (assoc op :value {:running :in-background, :cumulative-writes @writes}))

      (teardown! [this test])

      n/Reflection
      (fs [_] #{:rando}))))

(defn rando-generator
  "A generator for rando operations."
  [opts]
  (->> (fn [test ctx]
         {:type :info, :f :rando, :value (rand-nth (:nodes test))})
       (gen/stagger (:interval opts))))

(defn rando-package
  "A combined nemesis package for injecting random writes into some table."
  [opts]
  (when ((:faults opts) :rando)
    {:nemesis (rando-nemesis opts)
     :generator (rando-generator opts)
     :perf #{{:name "rando"
              :fs [:rando]
              :color "#E9A0E6"}}}))

(defn nemesis-package
  "Takes nemesis options (see jepsen.nemesis.combined for details and examples)
  and constructs a package of nemesis and generators."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        ; TODO: custom nemeses--port these over from jepsen.voltdb.
        (concat [(rando-package opts)])
        (->> (remove nil?))
        nc/compose-packages)))
