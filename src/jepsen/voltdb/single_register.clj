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
  ([] (client nil))
  ([conn]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [_ test node]
         (let [conn (voltdb/connect node)]
           (c/on node
                 (when (deliver initialized? true)
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
                   (info node "table created")))
           (client conn)))

       (invoke! [this test op]
         (let [id     (key (:value op))
               value  (val (:value op))]
           (case (:f op)
             :read   (let [v (-> conn
                                 (voltdb/call! "REGISTERS.select" id)
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
;                                  (ad-hoc! "UPDATE registers SET value = ?
;                                           WHERE id = ? AND value = ?"
;                                           v' id v)
                                  first
                                  :rows
                                  first
                                  :modified_tuples)]
                      (assert (#{0 1} res))
                      (assoc op :type (if (zero? res) :fail :ok))))))

       (teardown! [_ test]
         (.close conn))))))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn single-register-test
  "Takes a tarball URL"
  [url]
  (assoc tests/noop-test
         :name    "voltdb"
         :os      debian/os
         :client  (client)
         :db      (voltdb/db url)
         :model   (model/cas-register 0)
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
                           (gen/seq (cycle [(gen/sleep 30)
                                            {:type :info :f :start}
                                            (gen/sleep 30)
                                            {:type :info :f :stop}])))
                         (gen/time-limit 40))))
