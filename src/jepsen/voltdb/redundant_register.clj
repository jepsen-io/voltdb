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
  ([n]
   (client n nil (promise) nil))
  ([n node initialized? conn]
   (reify client/Client
     (open! [_ test node]
       (client n node initialized? (vc/connect node)))

     (setup! [_ test]
       (when (deliver initialized? true)
         (c/on node
               (vc/with-race-retry
                 ; Create table
                 (voltdb/sql-cmd! "CREATE TABLE rregisters (
                                  id            INTEGER NOT NULL,
                                  copy          INTEGER NOT NULL,
                                  value         INTEGER NOT NULL,
                                  PRIMARY KEY   (id, copy)
                                  );
                                  PARTITION TABLE rregisters ON COLUMN copy;")
                 (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS jepsen.procedures.RRegisterUpsert;")))))

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

     (teardown! [_ test])

     (close! [_ test]
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

(defn workload
  "Takes CLI options and returns a workload for testing redundant registers, to
  be merged into a test map."
  [opts]
  (let [n (count (:nodes opts))]
    {:client   (client n)
     :checker   (independent/checker (atomic-checker n))
     :generator (independent/concurrent-generator
                               (* 2 n)
                               (range)
                               (fn [id]
                                 (->> w
                                      (gen/reserve n r))))}))
