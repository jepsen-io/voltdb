(ns jepsen.voltdb.multi
  "A system of multiple registers. Verifies linearizability over each system."
  (:require [jepsen [core           :as jepsen]
                    [control        :as c :refer [|]]
                    [checker        :as checker]
                    [client         :as client]
                    [generator      :as gen]
                    [independent    :as independent]
                    [nemesis        :as nemesis]
                    [tests          :as tests]]
            [jepsen.os.debian       :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.voltdb          :as voltdb]
            [knossos.model          :as model]
            [knossos.op             :as op]
            [clojure.string         :as str]
            [clojure.pprint         :refer [pprint]]
            [clojure.core.reducers  :as r]
            [clojure.tools.logging  :refer [info warn]])
  (:import (knossos.model Model)))

(defn read-only?
  "Is a transaction a read-only transaction?"
  [txn]
  (every? #{:read} (map first txn)))

(defn client
  "A multi-register client. Options:

      :keys                         List of keys to create
      :system-count                 How many systems to preallocate
      :procedure-call-timeout       How long in ms to wait for proc calls
      :connection-response-timeout  How long in ms to wait for connections"
  ([opts] (client nil opts))
  ([conn opts]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [_ test node]
         (let [conn (voltdb/connect
                      node (select-keys opts
                                        [:procedure-call-timeout
                                         :connection-response-timeout]))]
           (when (deliver initialized? true)
             (try
               (c/on node
                     ; Create table
                     (voltdb/sql-cmd! "CREATE TABLE multi (
                                      system      INTEGER NOT NULL,
                                      key         VARCHAR NOT NULL,
                                      value       INTEGER NOT NULL,
                                      PRIMARY KEY (system, key)
                                      );
                                      PARTITION TABLE multi ON COLUMN key;")
                     (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                                      jepsen.procedures.MultiTxn;")
                     (info node "table created")
                     ; Create initial systems
                     (dotimes [i (:system-count opts)]
                       (doseq [k (:keys opts)]
                         (voltdb/call! conn "MULTI.insert" i (name k) 0)))
                     (info node "initial state populated"))
               (catch RuntimeException e
                 (voltdb/close! conn)
                 (throw e))))
           (client conn opts)))

       (invoke! [this test op]
         (try
           (case (:f op)
             :txn (let [[system txn] (:value op)
                        fs (->> txn
                                (map first)
                                (map name)
                                (into-array String))
                        ks (->> txn
                                (map second)
                                (map name)
                                (into-array String))
                        vs (->> txn
                                (map #(or (nth % 2) -1)) ; gotta pick an int
                                (into-array Integer/TYPE))
                        res (-> conn
                                (voltdb/call! "MultiTxn" system fs ks vs))
                        ; Map results of reads back into read values
                        txn' (mapv (fn [[f k v :as op] table]
                                     (case f
                                       :write op
                                       :read  (->> table :rows first :VALUE
                                                  (assoc op 2))))
                                   txn
                                   res)]
                    (assoc op
                           :type :ok
                           :value (independent/tuple system txn'))))
           (catch org.voltdb.client.NoConnectionsException e
             (assoc op :type :fail, :error :no-conns))
           (catch org.voltdb.client.ProcCallException e
             (let [type (if (read-only? (val (:value op))) :fail :info)]
               (condp re-find (.getMessage e)
                 #"^No response received in the allotted time"
                 (assoc op :type type, :error :timeout)

                 #"^Connection to database host .+ was lost before a response"
                 (assoc op :type type, :error :conn-lost)

                 #"^Transaction dropped due to change in mastership"
                 (assoc op :type type, :error :mastership-change)

                 (throw e))))))

       (teardown! [_ test]
         (voltdb/close! conn))))))

(defn op
  "An op is a tuple of [f k v] like [:read 0 nil], or [:write 2 3]"
  [k]
  (if (< (rand) 0.5)
    [:write k (rand-int 3)]
    [:read  k nil]))

(defn op-with-read
  "Like op, but yields sequences of transactions, prepending reads to writes.
  Helps us catch read errors faster, since writes are always legal."
  [k]
  (let [[f k v :as op] (op k)]
    (if (= f :read)
      [op]
      [[:read k nil] op])))

(defn txn
  "A transaction is a sequence of [type k v] tuples, e.g. [[:read 0
  3], [:write 1 2]]. For grins, we always perform a read before a write. Yields
  a generator of transactions over key-count registers."
  [ks]
  (let [ks (take (inc (rand-int (count ks))) (shuffle ks))]
    (vec (mapcat op-with-read ks))))

(defn txn-gen
  "A generator of transactions on ks"
  [ks]
  (fn [_ _] {:type :invoke, :f :txn, :value (txn ks)}))

(defn read-only-txn-gen
  "Generator for read-only transactions."
  [ks]
  (fn [_ _]
    {:type  :invoke
     :f     :txn
     :value (mapv (fn [k] [:read k nil]) ks)}))

(defn multi-test
  "Options:

      :time-limit                   How long should we run the test for?
      :tarball                      URL to an enterprise voltdb tarball.
      :procedure-call-timeout       How long in ms to wait for proc calls
      :connection-response-timeout  How long in ms to wait for connections"
  [opts]
  (let [ks [:x :y]
        system-count 1000]
    (merge tests/noop-test
           opts
           {:name    "voltdb"
            :os      debian/os
            :client  (client (merge
                               {:keys         ks
                                :system-count system-count}
                               (select-keys opts
                                            [:keys
                                             :system-count
                                             :procedure-call-timeout
                                             :connection-response-timeout])))
            :db      (voltdb/db (:tarball opts))
            :model   (model/multi-register (zipmap ks (repeat 0)))
            :checker (checker/compose
                       {:linear   (independent/checker checker/linearizable)
                        :timeline (independent/checker (timeline/html))
                        :perf     (checker/perf)})
            :nemesis (voltdb/with-recover-nemesis
                       (voltdb/isolated-killer-nemesis))
            :concurrency 100
            :generator (->> (independent/concurrent-generator
                              10
                              (range)
                              (fn [id]
                                (->> (txn-gen ks)
                                     (gen/reserve 5 (read-only-txn-gen ks))
                                     (gen/delay 1)
                                     (gen/time-limit 30))))
                            (voltdb/start-stop-recover-gen)
                            (gen/time-limit (:time-limit opts)))})))
