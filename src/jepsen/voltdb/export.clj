(ns jepsen.voltdb.export
  "A workload for testing VoltDB's export mechanism. We perform a series of
  write operations. Each write op performs a single transactional procedure
  call which inserts a series of values into both a VoltDB table and an
  exported stream.

    {:f :write, :values [3 4]}

  At the end of the test we read the table (so we know what VoltDB thinks
  happened).

    {:f :read-db, :values [1 2 3 4 ...]}

  ... and the exported data from the stream (so we know what was exported).

    {:f :read-export, :values [1 2 ...]}

  We then compare the two to make sure that records aren't lost, and spurious
  records don't appear in the export."
  (:require [clojure
             [pprint :refer [pprint]]
             [set          :as set]
             [string       :as str]]
            [clojure.core.reducers :as r]
            [clojure.tools.logging :refer [info warn]]
            [jepsen
             [checker      :as checker]
             [client       :as client]
             [control      :as c]
             [generator    :as gen]
             [history      :as h]]
            [jepsen.voltdb        :as voltdb]
            [jepsen.voltdb [client :as vc]]))

(defrecord Client [table-name     ; The name of the table we write to
                   stream-name    ; The name of the stream we write to
                   target-name    ; The name of our export target
                   conn           ; Our VoltDB client connection
                   node           ; The node we're talking to
                   initialized?   ; Have we performed one-time initialization?
                   ]
  client/Client
  (open! [this test node]
    (assoc this
           :conn (vc/connect node test)
           :node node))

  (setup! [_ test]
    (when (deliver initialized? true)
      (info node "Creating tables")
      (c/on node
            (vc/with-race-retry
              ; I'm not exactly sure what to do here--we want to test
              ; partitioned tables, I think, so we'll have an explicit
              ; partition column and send all our writes to one partition.
              ;
              ; The `value` column will actually store written values.
              (voltdb/sql-cmd! (str
                "CREATE TABLE " table-name " (
                part   INTEGER NOT NULL,
                value  BIGINT NOT NULL
                );
                PARTITION TABLE " table-name " ON COLUMN value;

                CREATE STREAM " stream-name " PARTITION ON COLUMN part
                EXPORT TO TARGET export_target (
                  part INTEGER NOT NULL,
                  value BIGINT NOT NULL
                );"))
              (voltdb/sql-cmd!
                (str "CREATE PROCEDURE FROM CLASS jepsen.procedures.ExportWrite;
                     PARTITION PROCEDURE ExportWrite ON TABLE " table-name " COLUMN part;"))
            (info node "tables created")))))

  (invoke! [_ test op]
    (try
      (case (:f op)
        ; Write to a random partition
        :write (do (vc/call! conn "ExportWrite"
                             (rand-int 1000)
                             (long-array (:value op)))
                   (assoc op :type :ok))

        ; TODO: implement this
        :db-read :unimplemented

        ; TODO: implement this
        :export-read :unimplemented
        )))

  (teardown! [_ test])

  (close! [_ test]
    (vc/close! conn)))

(defn rand-int-chunks
  "A lazy sequence of sequential integers grouped into randomly sized small
  vectors like [1 2] [3 4 5 6] [7] ..."
  ([] (rand-int-chunks 0))
  ([start]
   (lazy-seq
     (let [chunk-size (inc (rand-int 16))
           end        (+ start chunk-size)
           chunk      (vec (range start end))]
       (cons chunk (rand-int-chunks end))))))

(defn checker
  "Basic safety checker. Just checks for set inclusion, not order or
  duplicates."
  []
  ; TODO: This is just a sketch; I haven't gotten to feed this actual results
  ; yet
  (reify checker/Checker
    (check [this test history opts]
      (let [; What elements were acknowledged to the client?
            client-ok (->> history
                           h/oks
                           (h/filter-f :write)
                           (mapcat :value)
                           (into (sorted-set)))
            ; Which elements did we tell the client had failed?
            client-failed (->> history
                               h/fails
                               (h/filter-f :write)
                               (mapcat :value)
                               (into (sorted-set)))
            ; Which elements showed up in the DB reads?
            read-db (->> history
                         h/oks
                         (h/filter-f :read-db)
                         (mapcat :value)
                         (into (sorted-set)))
            ; Which elements showed up in the export?
            read-export (->> history
                             h/oks
                             (h/filter-f :read-export)
                             (mapcat :value)
                             (into (sorted-set)))
            ; Did we lose any writes confirmed to the client?
            lost          (set/difference client-ok read-db)
            ; How far behind the confirmed writes is the table?
            db-unseen     (set/difference read-db read-export)
            ; How far behind the table is the export?
            export-unseen (set/difference read-db read-export)
            ; Writes present in the export but the client thought they failed
            exported-but-client-failed (set/intersection read-export
                                                         client-failed)
           ; Writes present in export but missing from DB
           exported-but-not-in-db (set/difference read-export read-db)]
        {:valid? (and (empty? lost)
                      (empty? exported-but-client-failed)
                      (empty? exported-but-not-in-db))
         :client-ok-count                  (count client-ok)
         :client-failed-count              (count client-failed)
         :read-db-count                    (count read-db)
         :read-export-count                (count read-export)
         :lost-count                       (count lost)
         :db-unseen-count                  (count db-unseen)
         :export-unseen-count              (count export-unseen)
         :exported-but-client-failed-count (count exported-but-client-failed)
         :exported-but-not-in-db-count     (count exported-but-not-in-db)
         :lost                             lost
         :exported-but-client-failed       exported-but-client-failed
         :exported-but-not-in-db           exported-but-not-in-db}))))

(defn workload
  "Takes CLI options and constructs a workload map."
  [opts]
  {:client (map->Client {:table-name  "export_table"
                         :stream-name "export_stream"
                         :target-name "export_target"
                         :initialized? (promise)})
   :generator       (->> (rand-int-chunks)
                         (map (fn [chunk]
                                {:f :write, :value chunk})))
   :final-generator (gen/each-thread
                      [(gen/until-ok {:f :read-db})
                       (gen/until-ok {:f :read-export})])
   :checker (checker)})
