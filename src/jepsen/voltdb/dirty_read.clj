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
                    [os           :as os]
                    [util         :as util]
                    [tests        :as tests]]
            [jepsen.generator.context :as gen.context]
            [jepsen.os.debian     :as debian]
            [jepsen.voltdb        :as voltdb]
            [jepsen.voltdb [client :as vc]]
            [knossos.model        :as model]
            [knossos.op           :as op]
            [clojure.string       :as str]
            [clojure.set          :as set]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]))

(defn client
  "A single-register client."
  ([opts] (client opts (promise) nil nil))
  ([opts initialized? node conn]
   (reify client/Client
     (open! [_ test node]
       (let [conn (vc/connect
                    node
                    (select-keys opts
                                 [:procedure-call-timeout
                                  :connection-response-timeout]))]
         (client opts initialized? node conn)))

     (setup! [_ test]
       (when (deliver initialized? true)
         (c/on node
               ; Create table
               (vc/with-race-retry
                 (voltdb/sql-cmd! "CREATE TABLE dirty_reads (
                                  id          INTEGER NOT NULL,
                                  PRIMARY KEY (id)
                                  );
                                  PARTITION TABLE dirty_reads ON COLUMN id;")
                 (voltdb/sql-cmd! "CREATE PROCEDURE FROM CLASS
                                  jepsen.procedures.DirtyReadStrongRead;")))
         (info node "table created")))

     (invoke! [this test op]
       (try
         (case (:f op)
           ; Race conditions ahoy, awful hack
           :rejoin (if (vc/up? node)
                     (assoc op :type :ok, :value :already-up)
                     (do (c/on node (voltdb/rejoin! test node))
                         (assoc op :type :ok, :value :rejoined)))

           :read (let [v (->> (:value op)
                              (vc/call! conn "DIRTY_READS.select")
                              first
                              :rows
                              (map :ID)
                              first)]
                   (assoc op :type (if v :ok :fail), :value v))

           :write (do (vc/call! conn "DIRTY_READS.insert" (:value op))
                      (assoc op :type :ok))

           :strong-read (->> (vc/call! conn "DirtyReadStrongRead")
                             first
                             :rows
                             (map :ID)
                             (into (sorted-set))
                             (assoc op :type :ok, :value)))
         (catch org.voltdb.client.NoConnectionsException e
           ; It'll take a few seconds to come back, might as well take a
           ; breather
           (Thread/sleep 1000)
           (assoc op :type :fail, :error :no-conns))
         (catch org.voltdb.client.ProcCallException e
           (assoc op :type :info, :error (.getMessage e)))))

     (teardown! [_ test])

     (close! [_ test]
       (vc/close! conn)))))

(defn checker
  "Verifies that we never read an element from a transaction which did not
  commmit (and hence was not visible in a final strong read).

  Also verifies that every successful write is present in the strong read set."
  []
  (reify checker/Checker
    (check [checker test history opts]
      (let [ok    (filter op/ok? history)
            writes (->> ok
                        (filter #(= :write (:f %)))
                        (map :value)
                        (into (sorted-set)))
            reads (->> ok
                       (filter #(= :read (:f %)))
                       (map :value)
                       (into (sorted-set)))
            strong-read-sets (->> ok
                                  (filter #(= :strong-read (:f %)))
                                  (map :value))
            strong-reads (reduce set/union strong-read-sets)
            unseen       (set/difference strong-reads reads)
            dirty        (set/difference reads strong-reads)
            lost         (set/difference writes strong-reads)]
        ; We expect one strong read per node
        (info :strong-read-sets (count strong-read-sets))
        (info :concurrency (:concurrency test))
        (assert (= (count strong-read-sets) (:concurrency test)))
        ; All strong reads had darn well better be equal
        (assert (apply = (map count (cons strong-reads strong-read-sets))))

        {:valid?            (and (empty? dirty) (empty? lost))
         :read-count        (count reads)
         :strong-read-count (count strong-reads)
         :unseen-count      (count unseen)
         :dirty-count       (count dirty)
         :dirty             dirty
         :lost-count        (count lost)
         :lost              lost}))))

(defrecord RWGen
  [last-write ; The value we wrote last
   in-flight] ; A vector of in-flight writes on each node; initally nil
  gen/Generator
  (update [this test context event]
    this)

  (op [this test context]
    (let [; Lazy initialization of in-flight vector once test is ready
          in-flight (or in-flight (vec (repeat (count (:nodes test)) 0)))
          ; Pick a free process
          process (gen.context/some-free-process context)]
      (if (nil? process)
        [:pending this]
        (let [thread  (gen.context/process->thread context process)
              ; What number node is that?
              n (mod process (count (:nodes test)))]
          (if (= thread n)
            ; The first node-count processes perform writes
            (let [last-write' (inc last-write)
                  in-flight'  (assoc in-flight n last-write')]
              [(gen/fill-in-op {:f :write, :value last-write'} context)
               (RWGen. last-write' in-flight')])
            ; Remaining processes try to read most recent writes
            [(gen/fill-in-op {:f :read, :value (nth in-flight n)} context)
             this]))))))

(defn rw-gen
  "While one process writes to a node, we want another process to see that the
  in-flight write is visible, in the instant before the node crashes."
  []
  (RWGen. -1 nil))

(defn dirty-read-test
  "Takes an options map. Special options, in addition to voltdb/base-test:

  :procedure-call-timeout       How long in ms to wait for proc calls
  :connection-response-timeout  How long in ms to wait for connections
  :time-limit                   How long to run test for, in seconds"
  [opts]
  (voltdb/base-test
    (assoc opts
           :name    "voltdb dirty-read"
           :client  (client opts)
           :model   (model/cas-register 0)
           :checker (checker/compose
                      {:dirty-reads (checker)
                       :perf   (checker/perf)})
           :nemesis (voltdb/general-nemesis)
           :concurrency 15 ; TODO: this should be scaled with test node count --KRK 2023
           :generator (gen/phases
                        (->> (rw-gen)
                             (gen/stagger 1/100)
                             (voltdb/general-gen opts))
                        (voltdb/final-recovery)
                        (gen/clients (gen/each-thread {:f :strong-read}))))))
