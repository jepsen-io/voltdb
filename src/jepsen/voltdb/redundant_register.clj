(ns jepsen.voltdb.redundant-register
  "Implements a single register, but stores that register in n keys at once,
  all of which should refer to the same value."
  (:require [jepsen [core         :as jepsen]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [tests        :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.voltdb        :as voltdb]
            [jepsen.voltdb [client :as vc]]
            [knossos.model        :as model]
            [knossos.op           :as op]
            [clojure.string       :as str]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]))

(defn client
  "A client which implements a register, identified by a key. The register is
  stored in n copies, all of which should agree."
  [n node conn]
  (let [initialized? (promise)]
    (reify client/Client
      (open! [_ test node]
        (client n node (vc/connect node)))

      (setup! [_ test node]
        (c/on node
              (when (deliver initialized? true)
                ; Create table
                (voltdb/sql-cmd! "CREATE TABLE rregisters (
                                    id            INTEGER NOT NULL,
                                    copy          INTEGER NOT NULL,
                                    value         INTEGER NOT NULL,
                                    PRIMARY KEY   (id, copy)
                                  );
                                  PARTITION TABLE rregisters ON COLUMN copy;")
                (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS jepsen.procedures.RRegisterUpsert;"))))

      (invoke! [this test op]
        (let [id    (key (:value op))
              value (val (:value op))]
          (case (:f op)
            :read   (let [v (->> (vc/ad-hoc! conn "SELECT value FROM rregisters WHERE id = ? ORDER BY copy ASC;" id)
                                first
                                :rows
                                (map :VALUE))]
                      (assoc op
                             :type :ok
                             :value (independent/tuple id v)))
            :write  (do (vc/call! conn "RRegisterUpsert"
                                  id (long-array (range n)) value)
                        (assoc op :type :ok)))))

      (teardown! [_ test]
        (vc/close! conn)))))


(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})

(defn atomic-checker
  "Verifies that every read shows n identical values for all copies."
  [n]
  (reify checker/Checker
    (check [this test history opts]
      (let [mixed-reads (->> history
                             (r/filter (fn [op]
                                         (let [vs (:value op)]
                                           (and (op/ok? op)
                                                (= :read (:f op))
                                                (not= [] vs)
                                                (or (not= n (count vs))
                                                    (apply not= vs))))))
                             (into []))]
        {:valid? (empty? mixed-reads)
         :mixed-reads mixed-reads}))))

(defn rregister-test
  "Takes a tarball URL"
  [url]
  (let [n 5]
    (assoc tests/noop-test
           :name    "voltdb redundant-register"
           :os      debian/os
           :client  (client n nil)
           :db      (voltdb/db url)
           :model   (model/cas-register 0)
           :checker (checker/compose
                      {:atomic (independent/checker (atomic-checker n))
                       :perf   (checker/perf)})
           :nemesis (nemesis/partition-random-halves)
           :concurrency 100
           :generator (->> (independent/concurrent-generator
                             10
                             (range)
                             (fn [id]
                               (->> w
                                    (gen/reserve 5 r)
                                    (gen/delay 1/100)
                                    (gen/time-limit 30))))
                           (gen/nemesis
                             (cycle [(gen/sleep 30)
                                     {:type :info :f :start}
                                     (gen/sleep 30)
                                     {:type :info :f :stop}]))
                           (gen/time-limit 200)))))
