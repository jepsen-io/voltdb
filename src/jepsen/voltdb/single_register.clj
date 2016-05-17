(ns jepsen.voltdb.single-register
  "Implements a table of single registers identified by id. Verifies
  linearizability over independent registers."
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
            [knossos.model        :as model]
            [knossos.op           :as op]
            [clojure.string       :as str]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]))

(defn client
  "A single-register client."
  ([opts] (client nil opts))
  ([conn opts]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [_ test node]
         (info node "setting up")
         (let [conn (voltdb/connect node {:procedure-call-timeout 60000})]
           (info node "connected")
           (when (deliver initialized? true)
             (try
               (c/on node
                     ; Create table
                     (voltdb/sql-cmd! "CREATE TABLE registers (
                                      id          INTEGER UNIQUE NOT NULL,
                                      value       INTEGER NOT NULL,
                                      PRIMARY KEY (id)
                                      );
                                      PARTITION TABLE registers ON COLUMN id;")
                     (voltdb/sql-cmd! "CREATE PROCEDURE registers_cas
                                      PARTITION ON TABLE registers COLUMN id
                                      AS
                                      UPDATE registers SET value = ?
                                      WHERE id = ? AND value = ?;")
                     (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                                      jepsen.procedures.SRegisterStrongRead;")
                     (info node "table created"))
               (catch RuntimeException e
                 (voltdb/close! conn)
                 (throw e))))
           (info node "setup complete")
           (client conn opts)))

       (invoke! [this test op]
         (try
           (let [id     (key (:value op))
                 value  (val (:value op))]
             (case (:f op)
               :read   (let [proc (if (:strong-read? opts)
                                    "SRegisterStrongRead"
                                    "REGISTERS.select")
                             v (-> conn
                                   (voltdb/call! proc id)
                                   first
                                   :rows
                                   first
                                   :VALUE)]
                         (assoc op
                                :type :ok
                                :value (independent/tuple id v)))
               :write (do (voltdb/call! conn "REGISTERS.upsert" id value)
                          (assoc op :type :ok))
               :cas   (let [[v v'] value
                            res (-> conn
                                    (voltdb/call! "registers_cas" v' id v)
                                    first
                                    :rows
                                    first
                                    :modified_tuples)]
                        (assert (#{0 1} res))
                        (assoc op :type (if (zero? res) :fail :ok)))))
           (catch org.voltdb.client.ProcCallException e
             (let [type (if (= :read (:f op)) :fail :info)]
               (if (re-find #"^No response received in the allotted time"
                            (.getMessage e))
                 (assoc op :type type, :error :timeout)
                 (throw e))))))

       (teardown! [_ test]
         (voltdb/close! conn))))))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn single-register-test
  "Takes a tarball URL"
  [url]
  (assoc tests/noop-test
         :name    "voltdb"
         :os      debian/os
         :client  (client {:strong-reads? false})
         :db      (voltdb/db url)
         :model   (model/cas-register nil)
         :checker (checker/compose
                    {:linear (independent/checker checker/linearizable)
                     :perf   (checker/perf)})
         :nemesis (nemesis/partition-random-halves)
         :concurrency 100
         :generator (->> (independent/concurrent-generator
                           10
                           (range)
                           (fn [id]
                             (->> (gen/mix [w cas])
                                  (gen/reserve 5 r)
                                  (gen/delay 1)
                                  (gen/time-limit 30))))
                         (gen/nemesis
                           (gen/seq (cycle [(gen/sleep 25)
                                            {:type :info :f :start}
                                            (gen/sleep 25)
                                            {:type :info :f :stop}])))
                         (gen/time-limit 60))))
