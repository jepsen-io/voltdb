(ns jepsen.voltdb.dirty-read
  "Searches for dirty reads. We're targeting cases where VoltDB allows an
  unreplicated write to be visible to local reads *before* it's fully
  replicated. Because VoltDB currently allows stale reads, we can't use a
  normal read to verify that a transaction actually failed to commit. Instead
  we finally perform a *strong* read--a select + idempotent update, which
  forces the coordinator to push the read through the usual strongly-consistent
  path. Serializability still allows this read to be arbitrarily stale, so we
  sleep and hope that the write prevents the transaction from being reordered
  to the past. The optimizer probably isn't THAT clever."
  (:require [jepsen [core         :as jepsen]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [tests        :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.voltdb        :as voltdb]
            [knossos.model        :as model]
            [knossos.op           :as op]
            [clojure.string       :as str]
            [clojure.set          :as set]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]))

(defn client
  "A single-register client."
  ([] (client nil nil))
  ([node conn]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [_ test node]
         (let [conn (voltdb/connect node)]
           (c/on node
                 (when (deliver initialized? true)
                   ; Create table
                   (voltdb/sql-cmd! "CREATE TABLE dirty_reads (
                                      id          INTEGER NOT NULL,
                                      PRIMARY KEY (id)
                                    );
                                    PARTITION TABLE dirty_reads ON COLUMN id;")
                   (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                                    jepsen.procedures.DirtyReadStrongRead;")
                   (info node "table created")))
           (client node conn)))

       (invoke! [this test op]
         (try
           (case (:f op)
             ; Race conditions ahoy, awful hack
             :rejoin (if (voltdb/up? node)
                       (assoc op :type :ok, :value :already-up)
                       (do (c/on node (voltdb/rejoin! test node))
                           (assoc op :type :ok, :value :rejoined)))

             :read (let [v (->> (:value op)
                                (voltdb/call! conn "DIRTY_READS.select")
                                first
                                :rows
                                (map :ID)
                                first)]
                        (assoc op :type (if v :ok :fail), :value v))

             :write (do (voltdb/call! conn "DIRTY_READS.insert" (:value op))
                        (assoc op :type :ok))

             :strong-read (->> (voltdb/call! conn "DirtyReadStrongRead")
                               first
                               :rows
                               (map :ID)
                               (into (sorted-set))
                               (assoc op :type :ok, :value)))
           (catch org.voltdb.client.NoConnectionsException e
             (assoc op :type :info, :error :no-conns))
           (catch org.voltdb.client.ProcCallException e
             (assoc op :type :info, :error (.getMessage e)))))

       (teardown! [_ test]
         (info "Closing conn" conn)
         (.close conn))))))

(defn checker
  "Verifies that we never read an element from a transaction which did not
  commmit (and hence was not visible in a final strong read)"
  []
  (reify checker/Checker
    (check [checker test model history opts]
      (let [ok    (filter op/ok? history)
            reads (->> ok
                       (filter #(= :read (:f %)))
                       (map :value)
                       (reduce conj #{}))
            strong-read-sets (->> ok
                                  (filter #(= :strong-read (:f %)))
                                  (map :value))
            strong-reads (reduce set/union strong-read-sets)
            missing     (set/difference reads strong-reads)]
        ; We expect at least one strong read
        (info :strong-read-sets (count strong-read-sets))
        (info :concurrency (:concurrency test))
        (assert (pos? (count strong-read-sets)))
        ; All strong reads had darn well better be equal
        (assert (apply = (map count (cons strong-reads strong-read-sets))))

        {:valid?            (empty? missing)
         :read-count        (count reads)
         :strong-read-count (count strong-reads)
         :missing-count     (count missing)
         :missing           missing}))))

(defn sr  [_ _] {:type :invoke, :f :strong-read, :value nil})

(defn rw-gen
  "While one process writes to a node, we want another process to see that the
  in-flight write is visible, in the instant before the node crashes."
  []
  (let [; What did we write last?
        write (atom -1)
        ; A vector of in-flight writes on each node.
        in-flight (atom nil)]
    (reify gen/Generator
      (op [_ test process]
        ; lazy init of in-flight state
        (when-not @in-flight
          (compare-and-set! in-flight
                            nil
                            (vec (repeat (count (:nodes test)) 0))))

        (let [; thread index
              t (gen/process->thread test process)
              ; node index
              n (mod process (count (:nodes test)))]
          (if (= t n)
            ; The first n processes perform writes
            (let [v (swap! write inc)]
              ; Record the in-progress write
              (swap! in-flight assoc n v)
              {:type :invoke, :f :write, :value v})

            ; Remaining processes try to read the most recent write
            {:type :invoke, :f :read, :value (nth @in-flight n)}))))))

(defn dirty-read-test
  "Takes a tarball URL"
  [url]
  (assoc tests/noop-test
         :name    "voltdb"
         :os      debian/os
         :client  (client)
         :db      (voltdb/db url)
         :model   (model/cas-register 0)
         :checker (checker/compose
                    {:dirty-reads (checker)
                     :perf   (checker/perf)})
         :nemesis (nemesis/node-start-stopper
                    #(take 1 (shuffle %))
                    (fn [test node]
                      ; Isolate this node
                      (nemesis/partition! test
                        (nemesis/complete-grudge
                          [[node] (remove #{node} (:nodes test))]))
                      (info "Partitioned away" node)
                      (Thread/sleep 5000)
                      (voltdb/stop! test node)
                      :killed)
                    (fn [test node]
                      (net/heal! (:net test) test)
                      (info "Network healed")
                      (voltdb/rejoin! test node)
                      :rejoined))
         :concurrency 10
         :generator (gen/phases
                      (->> (rw-gen)
                           (gen/nemesis
                             (gen/seq (cycle [(gen/sleep 1)
                                              {:type :info :f :start}
                                              {:type :info :f :stop}])))
                           (gen/time-limit 60))
                      (gen/nemesis (gen/once {:type :info :f :stop}))
                      (gen/log "Rejoining all down nodes")
                      (gen/clients
                        (gen/singlethreaded
                          (gen/each
                            (gen/once {:type :invoke :f    :rejoin}))))
                      (gen/log "Waiting for reconnects")
                      (gen/sleep 10)
                      (gen/clients (gen/each (gen/once sr))))))
