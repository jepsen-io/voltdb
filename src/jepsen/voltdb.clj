(ns jepsen.voltdb
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [net          :as net]
                    [os           :as os]
                    [tests        :as tests]
                    [util         :as util :refer [meh timeout]]]
            [jepsen.os            :as os]
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [knossos.model        :as model]
            [clojure.data.xml     :as xml]
            [clojure.string       :as str]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]])
  (:import (org.voltdb VoltTable
                       VoltType
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse
                              ProcedureCallback)))

(def username "voltdb")
(def base-dir "/opt/voltdb")
(def voltroot (str base-dir "/voltdbroot"))

(defn os
  "Given OS, plus python & jdk8"
  [os]
  (reify os/OS
    (setup! [_ test node]
      (os/setup! os test node)
      (debian/install ["python2.7"])
      (c/exec :update-alternatives :--install "/usr/bin/python" "python"
              "/usr/bin/python2.7" 1)
      (debian/install-jdk8!)
      (info "JDK8 installed"))

    (teardown! [_ test node]
      (os/teardown! os test node))))

(defn install!
  "Install the given tarball URL"
  [node url force?]
  (c/su
    (cu/install-tarball! node url base-dir force?)
    (c/exec :mkdir (str base-dir "/log"))
    (c/exec :mkdir :-p (str voltroot "/log"))
    (cu/ensure-user! username)
    (c/exec :chown :-R (str username ":" username) base-dir)

    (info "VoltDB unpacked")))

(defn deployment-xml
  "Generate a deployment.xml string for the given test."
  [test]
  (xml/emit-str
    (xml/sexp-as-element
      [:deployment {}
       [:cluster {:hostcount (count (:nodes test))
;                  :sitesperhost 2
                  :kfactor (min 4 (dec (count (:nodes test))))}]
       [:paths {}
        [:voltdbroot {:path base-dir}]]
       ; We need to choose a heartbeat high enough so that we can spam
       ; isolated nodes with requests *before* they kill themselves
       ; but low enough that a new majority is elected and performs
       ; some operations.
       [:heartbeat {:timeout 2}] ; seconds
       [:commandlog {:enabled true, :synchronous true, :logsize 128}
        [:frequency {:time 2}]]]))) ; milliseconds

(defn init-voltdb!
  "Starts the daemon with the given command."
  [test host]
  (info host "initializing voltdbroot")
  (c/sudo username
          (c/cd voltroot
                (c/exec (str base-dir "/bin/voltdb") :init
                    :--config (str base-dir "/deployment.xml")
                    :--force
                    :--dir (str base-dir)
                    (c/lit ">> log/stdout.log")
                ))))

(defn configure!
  "Prepares config files and creates fresh DB."
  [test node]
  (c/sudo username
        (c/cd base-dir
              (c/exec :echo (deployment-xml test) :> "deployment.xml")))

  (init-voltdb! (jepsen/primary test))
)

(defn close!
  "Calls c.close"
  [^Client c]
  (.close c))

(defn up?
  "Is the given node ready to accept connections? Returns node, or nil."
  [node]
  (let [config (ClientConfig. "" "")]
    (.setProcedureCallTimeout config 100)
    (.setConnectionResponseTimeout config 100)

    (let [c (ClientFactory/createClient config)]
      (try
        (.createConnection c (name node))
        (.getInstanceId c)
        node
      (catch java.net.ConnectException e)
      (finally (close! c))))))

(defn up-nodes
  "What DB nodes are actually alive?"
  [test]
  (remove nil? (pmap up? (:nodes test))))

(defn await-start
    "Blocks until the logfile reports 'Server completed initialization'."
  [node]
  (info "Waiting for" node "to start at voltroot:" voltroot )
  (def done false)

       (loop [x 50]
       (try
           (c/sudo username
                   (c/cd voltroot

                         (Thread/sleep 5000)
                         (info node (c/exec :tail :-n 5 "log/volt.log"))
                         (c/exec :tail :-n 5 "log/volt.log"
                                 | :grep :-m 1 :-E "completed initialization|Node rejoin completed"
                                 )))

                   (info node "started")
                   (def done true )
	       (catch Exception e (info node "waiting for startup " x " retries left" ) )
       )

       (when (and (not done) (> x 0))
            (recur (dec x)))
       )
       (when (not done)
            (throw (RuntimeException.
                    (str node " failed to start in time; STDOUT:\n\n"
                        (meh (c/exec :tail :-n 10
                                      (str base-dir "/log/stdout.log")))
                         "\n\nLOG:\n\n"
                         (meh (c/exec :tail :-n 10
                                      (str voltroot "/log/volt.log")))
                         "\n\n")))
        )
   "done"
)

(defn await-rejoin
  "Blocks until the logfile reports 'Node rejoin completed'"
  [node]
  (info "Waiting for" node "to rejoin")
  (def done 0)
  (timeout 240000
           (throw (RuntimeException.
                    (str node " failed to rejoin in time; STDOUT:\n\n"
                        (meh (c/exec :tail :-n 10
                                      (str base-dir "/log/stdout.log")))
                         "\n\nLOG:\n\n"
                         (meh (c/exec :tail :-n 10
                                      (str voltroot "/log/volt.log")))
                         "\n\n")))


  (loop [x 100]
  (info node " NOT rejoined, " x "attempts left")
  (try
  (c/sudo username
          (c/cd voltroot
                ; hack hack hack
                (Thread/sleep 2000)
                (c/exec :tail :-n 1 :-f "log/volt.log"
                        | :grep :-m 1 "Node rejoin completed"
                        | :xargs (c/lit "echo \"\" >> log/volt.log \\;")))
          (info node "rejoined"))
          ( def done 1 )

   (catch RuntimeException e (info node "waiting for rejoin" ) )
   )

   (when (and (> x 0 ) (= done 0))
   	(recur (dec x)))
   )
   )
)



(defn start-daemon!
  "Starts the daemon with the given command."
  [test cmd host]
  (c/sudo username
          (c/cd base-dir
                (cu/start-daemon! {:logfile (str base-dir "/log/stdout.log")
                                   :pidfile (str base-dir "/pidfile")
                                   :chdir   base-dir}
                                  (str base-dir "/bin/voltdb")
                                  (str "start")
                                  :--dir (str base-dir)
                                  :--count (count (:nodes test))
                                  :--host host))))

(defn start!
  "Starts voltdb, creating a fresh DB"
  [test node]
  (start-daemon! test :start (jepsen/primary test))
  (await-start node))

(defn recover!
  "Recovers an entire cluster, or with a node, a single node."
  ([test]
   (c/on-nodes test recover!))
  ([test node]
   (info "recovering" node)
   (start-daemon! test :recover (jepsen/primary test))
   (await-start node)))

(defn rejoin!
  "Rejoins a voltdb node. Serialized to work around a bug in voltdb where
  multiple rejoins can take down cluster nodes."
  [test node]
  (locking rejoin!
    (info "rejoining" node)
    (start-daemon! test :rejoin (rand-nth (up-nodes test)))
    (await-rejoin node)))

(defn stop!
  "Stops voltdb"
  [test node]
  (c/su
    (cu/stop-daemon! (str base-dir "/pidfile"))))

(defn stop-recover!
  "Stops all nodes, then recovers all nodes. Useful when Volt's lost majority
  and nodes kill themselves."
  ([test]
   (c/on-nodes test stop!)
   (c/on-nodes test recover!)))

(defn sql-cmd!
  "Takes an SQL query and runs it on the local node via sqlcmd"
  [query]
  (c/cd base-dir
        (c/sudo username
                (c/exec "bin/sqlcmd" (str "--query=" query)))))

(defn snarf-procedure-deps!
  "Downloads voltdb.jar from the current node to procedures/, so we can compile
  stored procedures."
  []
  (let [dir  (str base-dir "/voltdb/")
        f    (first (c/cd dir (cu/ls (c/lit "voltdb-*.jar"))))
        src  (str dir f)
        dest (io/file (str "procedures/" f))]
    (when-not (.exists dest)
      (info "Downloading" f "to" (.getCanonicalPath dest))
      (c/download src (.getCanonicalPath dest)))))

(defn build-stored-procedures!
  "Compiles and packages stored procedures in procedures/"
  []
  (sh "mkdir" "obj" :dir "procedures/")
  (let [r (sh "bash" "-c" "javac -classpath \"./:./*\" -d ./obj *.java"
              :dir "procedures/")]
    (when-not (zero? (:exit r))
      (throw (RuntimeException. (str "STDOUT:\n" (:out r)
                                     "\n\nSTDERR:\n" (:err r))))))
  (let [r (sh "jar" "cvf" "jepsen-procedures.jar" "-C" "obj" "."
              :dir "procedures/")]
    (when-not (zero? (:exit r))
      (throw (RuntimeException. (str "STDOUT:\n" (:out r)
                                     "\n\nSTDERR:\n" (:err r)))))))

(defn upload-stored-procedures!
  "Uploads stored procedures jar."
  [node]
  (c/upload (.getCanonicalPath (io/file "procedures/jepsen-procedures.jar"))
            (str base-dir "/jepsen-procedures.jar"))
  (info node "stored procedures uploaded"))

(defn load-stored-procedures!
  "Load stored procedures into voltdb."
  [node]
  (sql-cmd! "load classes jepsen-procedures.jar")
  (info node "stored procedures loaded"))

(defn kill-reconnect-threads!
  "VoltDB client leaks reconnect threads; this kills them all."
  []
  (doseq [t (keys (Thread/getAllStackTraces))]
    (when (= "Retry Connection" (.getName t))
      ; The reconnect loop swallows Exception so we can't even use interrupt
      ; here. Luckily I don't think it has too many locks we have to worry
      ; about.
      (.stop t))))

(defn db
  "VoltDB around the given package tarball URL"
  [url force-download?]
  (reify db/DB
    (setup! [_ test node]
      ; Download and unpack
      (install! node url force-download?)

      ; Prepare stored procedures in parallel
      (let [procedures (future (when (= node (jepsen/primary test))
                                 (snarf-procedure-deps!)
                                 (build-stored-procedures!)
                                 (upload-stored-procedures! node)))]
        ; Boot
        (configure! test node)
        (start! test node)

        ; Wait for convergence
        (jepsen/synchronize test)

        ; Finish procedures
        @procedures
        (when (= node (jepsen/primary test))
          (load-stored-procedures! node))))

    (teardown! [_ test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf (c/lit (str base-dir "/*"))))
      (kill-reconnect-threads!))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/log/stdout.log")
       (str voltroot "/log/volt.log")])))


(defn connect
  "Opens a connection to the given node and returns a voltdb client. Options:

      :reconnect?
      :procedure-call-timeout
      :connection-response-timeout"
  ([node]
   (connect node {}))
  ([node opts]
   (let [opts (merge {:procedure-call-timeout 100
                      :connection-response-timeout 1000}
                     opts)]
     (-> (doto (ClientConfig. "" "")
           (.setReconnectOnConnectionLoss (get opts :reconnect? true))
           (.setProcedureCallTimeout (:procedure-call-timeout opts))
           (.setConnectionResponseTimeout (:connection-response-timeout opts)))
         (ClientFactory/createClient)
         (doto
           (.createConnection (name node)))))))

(defn volt-table->map
  "Converts a VoltDB table to a data structure like

  {:status status-code
   :schema [{:column_name VoltType, ...}]
   :rows [{:k1 v1, :k2 v2}, ...]}"
  [^VoltTable t]
  (let [column-count (.getColumnCount t)
        column-names (loop [i     0
                            cols  (transient [])]
                       (if (= i column-count)
                         (persistent! cols)
                         (recur (inc i)
                                (conj! cols (keyword (.getColumnName t i))))))
        basis        (apply create-struct column-names)
        column-types (loop [i 0
                            types (transient [])]
                       (if (= i column-count)
                         (persistent! types)
                         (recur (inc i)
                                (conj! types (.getColumnType t i)))))
        row          (doto (.cloneRow t)
                       (.resetRowPosition))]
  {:status (.getStatusCode t)
   :schema (apply struct basis column-types)
   :rows (loop [rows (transient [])]
           (if (.advanceRow row)
             (let [cols (object-array column-count)]
               (loop [j 0]
                 (when (< j column-count)
                   (aset cols j (.get row j ^VoltType (nth column-types j)))
                   (recur (inc j))))
               (recur (conj! rows (clojure.lang.PersistentStructMap/construct
                                    basis
                                    (seq cols)))))
             ; Done
             (persistent! rows)))}))

(defn call!
  "Call a stored procedure and returns a seq of VoltTable results."
  [^Client client procedure & args]
  (let [res (.callProcedure client procedure (into-array Object args))]
    ; Docs claim callProcedure will throw, but tutorial checks anyway so ???
    (assert (= (.getStatus res) ClientResponse/SUCCESS))
    (map volt-table->map (.getResults res))))

(defn async-call!
  "Call a stored procedure asynchronously. Returns a promise of a seq of
  VoltTable results. If a final fn is given, passes ClientResponse to that fn."
  [^Client client procedure & args]
  (let [p (promise)]
    (.callProcedure client
                    (reify ProcedureCallback
                      (clientCallback [this res]
                        (when (fn? (last args))
                          ((last args) res))
                        (deliver p (map volt-table->map (.getResults res)))))
                    procedure
                    (into-array Object (if (fn? (last args))
                                         (butlast args)
                                         args)))))

(defn ad-hoc!
  "Run an ad-hoc SQL stored procedure."
  [client & args]
  (apply call! client "@AdHoc" args))

;; Nemeses

(defn killer-nemesis
  "A nemesis which kills the given collection of nodes on :start, and rejoins
  them on :stop."
  []
  (reify client/Client
    (setup! [this test _] this)

    (invoke! [this test op]
      (case (:f op)
        :start (do (dorun (util/real-pmap (partial stop! test) (:value op)))
                   [:killed (:value op)])
        :stop  (->> (:value op)
                    (map (fn [node]
                           [node (if (up? node)
                                   :already-up
                                   (do (rejoin! test node)
                                       :rejoined))])
                         (into (sorted-set))))))
    (teardown! [this test])))

(defn isolator-nemesis
  "A nemesis which handles :start by isolating the nodes in the op's :value
  into their own partition, and :stop by healing the network."
  []
  (reify client/Client
    (setup! [this test _]
      (net/heal! (:net test) test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (let [grudge (nemesis/complete-grudge
                              [(:value op)
                               (remove (set (:value op)) (:nodes test))])]
                 (nemesis/partition! test grudge)
                 (assoc op :value [:partitioned grudge]))
        :stop (do (net/heal! (:net test) test)
                  (assoc op :value :fully-connected))))

    (teardown! [this test]
      (net/heal! (:net test) test))))

(defn recover-nemesis
  "A nemesis which responds to :recover ops by healing the network, killing and
  recovering all nodes in the test."
  []
  (reify client/Client
    (setup! [this test _] this)

    (invoke! [this test op]
      (assoc op :type :info, :value
             (case (:f op)
               :recover (do (net/heal! (:net test) test)
                            (stop-recover! test)
                            [:recovered (:nodes test)]))))

    (teardown! [this test])))

(defn rando-nemesis
  "Confuses VoltDB by spewing random writes into an unrelated table, on one of
  the nodes in (:value op)"
  ([]
   (let [initialized? (promise)
         writes       (atom 0)]
     (reify client/Client
       (setup! [this test _]
         (let [node (first (:nodes test))
               conn (connect node {})]
           (when (deliver initialized? true)
             (try
               (c/on node
                     ; Create table
                     (sql-cmd! "CREATE TABLE mentions (
                                 well     INTEGER NOT NULL,
                                 actually INTEGER NOT NULL
                               );")
                     (info node "mentions table created"))
               (finally
                 (close! conn)))))
         this)

       (invoke! [this test op]
         (assert (= :rando (:f op)))
         (let [conn (connect (first (:value op))
                             {:procedure-call-timeout 100
                              :reconnect? false})]
           (future
             (try
               (dotimes [i 50000]
;                 (call! conn "MENTIONS.insert" i (rand-int 1000))
                 ; If we go TOO fast we'll start forcing other ops to time
                 ; out. If we go too slow we won't get a long enough log.
                 (Thread/sleep 1)
                 (async-call!
                   conn "MENTIONS.insert" i (rand-int 1000)
                   (fn [^ClientResponse res]
                     (when (or (= ClientResponse/SUCCESS (.getStatus res))
                               ; not sure why this happens but it's ok?
                               (= ClientResponse/UNINITIALIZED_APP_STATUS_CODE
                                  (.getStatus res)))
                       (->> res
                            .getResults
                            (map volt-table->map)
                            first
                            :rows
                            first
                            :modified_tuples
                            (swap! writes +))))))
               (catch Exception e
                 (info "Rando nemesis finished with" (.getMessage e)))
               (finally
                 (info "Rando nemesis finished writing")
                 (close! conn))))
           (assoc op :value @writes)))

       (teardown! [this test])))))

(defn general-nemesis
  "Performs kill/recover recovery, network partitions,
  isolated+kill/rejoins, and random operations."
  []
  (nemesis/compose
    {#{:recover}        (recover-nemesis)
     #{:rando}          (rando-nemesis)
     {:isolate :start
      :heal    :stop}   (isolator-nemesis)
     {:kill    :start
      :rejoin  :stop}   (killer-nemesis)}))

;; Generators

(defn simple-partition-gen
  "A generator for a nemesis with simple partitions: 25 seconds on, 25 seconds
  off. Wraps a client gen."
  [gen]
  (gen/nemesis (gen/seq (cycle [(gen/sleep 25)
                                {:type :info, :f :start}
                                (gen/sleep 25)
                                {:type :info, :f :stop}]))
               gen))

(defn rando-gen
  "Emits rando operations on random nodes."
  [test process]
  (when-let [up (seq (filter up? (:nodes test)))]
    {:type  :info
     :f     :rando
     :value (rand-nth up)}))

(defn random-minority
  "A random minority subset of a collection, at least one member."
  [coll]
  (-> coll
      count
      (/ 2)
      Math/ceil
      dec
      rand-int
      inc
      (take (shuffle coll))))

(defn isolate-thirst-kill-gen
  "Isolates a minority set of nodes, spams them with random operations, then
  performs global recovery. Takes a recovery delay in seconds."
  [recovery-delay]
  (let [state      (atom {:type :info, :f :recover, :value nil})
        transition (fn [s test]
                     (case (:f s)
                       :recover (assoc s :f :rando
                                         :value (random-minority (:nodes test)))
                       :rando   (assoc s :f :isolate)
                       :isolate (assoc s :f :recover, :value nil)))]
    (reify gen/Generator
      (op [_ test process]
        (let [op (swap! state transition test)]
          (when (and recovery-delay (= :recover (:f op)))
            (Thread/sleep (* 1000 recovery-delay)))
          op)))))

(defn general-gen
  "Emits a random mixture of partitions, node failures/rejoins, and recoveries,
  for (:time-limit test) seconds. Allocates one process as a rando. Wraps an
  underlying client generator."
  [opts gen]
  (->> gen
       (gen/nemesis
         (gen/phases
           (gen/sleep 5)
           (isolate-thirst-kill-gen (:recovery-delay opts))))
       (gen/time-limit (:time-limit opts))))

(defn final-recovery
  "A generator which emits a :stop, followed by a :recover, for the nemesis,
  then sleeps to allow clients to reconnect."
  []
  (gen/phases
    (gen/log "Recovering cluster")
    (gen/nemesis
      (gen/once {:type :info, :f :recover}))
    (gen/log "Waiting for reconnects")
    (gen/sleep 10)))

(defn base-test
  "Constructs a basic test case with common options.

        :tarball                      URL to an enterprise voltdb tarball
        :skip-os?                     Skip OS setup
        :force-download?              Always download tarball URL
        :nodes                        Nodes to run against"
  [opts]
  (-> tests/noop-test
      (assoc :os (if (:skip-os? opts) os/noop (os debian/os)))
      (assoc :db (db (:tarball opts) (:force-download? opts)))
      (merge opts)))
