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
                    [util         :as util :refer [await-fn meh timeout]]]
            [jepsen.os            :as os]
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [jepsen.voltdb.client :as vc]
            [knossos.model        :as model]
            [clojure.data.xml     :as xml]
            [clojure.string       :as str]
            [clojure.java.io      :as io]
            [clojure.java.shell   :refer [sh]]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (org.voltdb VoltTable
                       VoltType
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse
                              ProcedureCallback)))

(def username "voltdb")
(def base-dir "/tmp/jepsen-voltdb")
(def client-port 21212)

(defn os
  "Given OS, plus python & jdk"
  [os]
  (reify os/OS
    (setup! [_ test node]
      (os/setup! os test node)
      (debian/install ["python2.7" "openjdk-17-jdk-headless"])
      (c/exec :update-alternatives :--install "/usr/bin/python" "python"
              "/usr/bin/python2.7" 1))

    (teardown! [_ test node]
      (os/teardown! os test node))))

(defn install!
  "Install the given tarball URL"
  [node url force?]
  (c/su
    (if-let [[m path _ filename] (re-find #"^file://((.*/)?([^/]+))$" url)]
      (do ; We're installing a local tarball from the control node; upload it.
          (c/exec :mkdir :-p "/tmp/jepsen")
          (let [remote-path (str "/tmp/jepsen/" filename)]
            (c/upload path remote-path)
            (cu/install-archive! (str "file://" remote-path)
                                 base-dir {:force? force?})))
      ; Probably an HTTP URI; just let install-archive handle it
      (cu/install-archive! url base-dir {:force? force?}))
    (c/exec :mkdir (str base-dir "/log"))
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
       ; TODO: consider changing commandlog enabled to false to speed up startup
       [:commandlog {:enabled true, :synchronous true, :logsize 128}
        [:frequency {:time 2}]]]))) ; milliseconds

(defn init-db!
  "run voltdb init"
  [node]
  (info "Initializing voltdb")
  (c/sudo username
        (c/cd base-dir
              (c/exec (str base-dir "/bin/voltdb") :init
              :--config (str base-dir "/deployment.xml")
             
              | :tee (str base-dir "/log/stdout.log"))))
  (info node "initialized"))

(defn configure!
  "Prepares config files and creates fresh DB."
  [test node]
  (c/sudo username
    (c/cd base-dir
          (c/upload (:license test) (str base-dir "/license.xml"))
          (cu/write-file! (deployment-xml test) "deployment.xml")
          (init-db! node)
          (c/exec :ln :-f :-s (str base-dir "/voltdbroot/log/volt.log") (str base-dir "/log/volt.log" )))))

(defn await-log
  "Blocks until voltdb.log contains the given string."
  [line]
  (let [file (str base-dir "/log/volt.log")]
    (c/sudo username
            (c/cd base-dir
                  ; There used to be a sleep here of *four minutes*. Why? --KRK
                  (c/exec :tail :-n 20 file
                          | :grep :-m 1 :-f line
                          ; What is this xargs FOR? What was I thinking seven
                          ; years ago? --KRK, 2023
                          | :xargs (c/lit (str "echo \"\" >> " file
                                               " \\;")))))))

(defn await-start
  "Blocks until the node is up, responding to client connections, and
  @SystemInformation OVERVIEW returns."
  [node]
  (info "Waiting for" node "to start")
  (cu/await-tcp-port client-port {:log-interval 10000
                                  :timeout 300000})
  (with-open [conn (vc/connect node {:procedure-call-timeout 100
                                     :reconnect? false})]
    ; Per Ruth, just being able to ask for SystemInformation should indicate
    ; the cluster is ready to use. We'll make sure we get at least one table
    ; back, just in case.
    (await-fn (fn check-system-info []
                (let [overview (vc/call! conn "@SystemInformation" "OVERVIEW")]
                  (when (empty? overview)
                    (throw+ {:type ::empty-overview}))))
              {:log-message "Waiting for @SystemInformation"
               :log-interval 10000
               :retry-interval 1000
               :timeout 240000}))
  (info node "started"))

(defn await-rejoin
  "Blocks until the logfile reports 'Node rejoin completed'"
  [node]
  (info "Waiting for" node "to rejoin")
  (await-log "Node rejoin completed")
  (info node "rejoined"))

(defn start-daemon!
  "Starts the daemon with the given command."
  [test cmd host]
  (c/sudo username
          (c/cd base-dir
                (cu/start-daemon! {:logfile (str base-dir "/log/stdout.log")
                                   :pidfile (str base-dir "/pidfile")
                                   :chdir   base-dir}
                                  (str base-dir "/bin/voltdb")
                                  cmd
                                  ; TODO: These shouldn't be hardcoded--also,
                                  ; didn't we have an explicit rejoin procedure
                                  ; for joining a specific node with --host?
                                  ; Should that still be the case, or is that
                                  ; obsolete? --KRK 2023
                                  :--count (str 5)
                                  :--host (str "n1,n2,n3,n4,n5")
                )
            )
  )
)

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
   (start-daemon! test :start (jepsen/primary test))
   (await-start node)))

(defn rejoin!
  "Rejoins a voltdb node. Serialized to work around a bug in voltdb where
  multiple rejoins can take down cluster nodes."
  [test node]
  (locking rejoin!
    (info "rejoining" node)
    (start-daemon! test :start (rand-nth (vc/up-nodes test)))
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
  ; Volt currently plans on JDK8, and we're concerned that running on 17 might
  ; be the cause of a bug. Just in case, we'll target compilation back to 11
  ; (the oldest version you can install on Debian Bookworm easily)
  (let [r (sh "bash" "-c" "javac -source 11 -target 11 -classpath \"./:./*\" -d ./obj *.java"
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
        (jepsen/synchronize test 240)

        ; Finish procedures
        @procedures
        (when (= node (jepsen/primary test))
          (load-stored-procedures! node))))

    (teardown! [_ test node]
      (stop! test node)
      (c/su
        (c/exec :rm :-rf (c/lit (str base-dir "/*"))))
      (vc/kill-reconnect-threads!))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/log/stdout.log")
       (str base-dir "/log/volt.log")])))

;; Nemeses

(defn killer-nemesis
  "A nemesis which kills the given collection of nodes on :start, and rejoins
  them on :stop."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

    (invoke! [this test op]
      (case (:f op)
        :start (do (dorun (util/real-pmap (partial stop! test) (:value op)))
                   [:killed (:value op)])
        :stop  (->> (:value op)
                    (map (fn [node]
                           [node (if (vc/up? node)
                                   :already-up
                                   (do (rejoin! test node)
                                       :rejoined))])
                         (into (sorted-set))))))
    (teardown! [this test])))

(defn isolator-nemesis
  "A nemesis which handles :start by isolating the nodes in the op's :value
  into their own partition, and :stop by healing the network."
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (net/heal! (:net test) test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (let [grudge (nemesis/complete-grudge
                              [(:value op)
                               (remove (set (:value op)) (:nodes test))])]
                 (net/drop-all! test grudge)
                 (assoc op :value [:partitioned grudge]))
        :stop (do (net/heal! (:net test) test)
                  (assoc op :value :fully-connected))))

    (teardown! [this test]
      (net/heal! (:net test) test))))

(defn recover-nemesis
  "A nemesis which responds to :recover ops by healing the network, killing and
  recovering all nodes in the test."
  []
  (reify nemesis/Nemesis
    (setup! [this test] this)

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
     (reify nemesis/Nemesis
       (setup! [this test]
         (let [node (first (:nodes test))
               conn (vc/connect node {})]
           (when (deliver initialized? true)
             (try
               (c/on node
                     (vc/with-race-retry
                       ; Create table
                       (sql-cmd! "CREATE TABLE mentions (
                                   well     INTEGER NOT NULL,
                                   actually INTEGER NOT NULL
                                 );"))
                     (info node "mentions table created"))
               (finally
                 (vc/close! conn)))))
         this)

       (invoke! [this test op]
         (assert (= :rando (:f op)))
         (let [conn (vc/connect (first (:value op))
                                {:procedure-call-timeout 100
                                 :reconnect? false})]
           (future
             (try
               (dotimes [i 50000]
;                 (call! conn "MENTIONS.insert" i (rand-int 1000)))
                 ; If we go TOO fast we'll start forcing other ops to time
                 ; out. If we go too slow we won't get a long enough log.
                 (Thread/sleep 1)
                 (vc/async-call!
                   conn "MENTIONS.insert" i (rand-int 1000)
                   (fn [^ClientResponse res]
                     (when (or (= ClientResponse/SUCCESS (.getStatus res))
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
                            (swap! writes +))))))
               (catch Exception e
                 (info "Rando nemesis finished with" (.getMessage e)))
               (finally
                 (info "Rando nemesis finished writing")
                 (vc/close! conn))))
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
  (gen/nemesis (cycle [(gen/sleep 25)
                       {:type :info, :f :start}
                       (gen/sleep 25)
                       {:type :info, :f :stop}])
               gen))

(defn rando-gen
  "Emits rando operations on random nodes."
  [test process]
  (when-let [up (seq (filter vc/up? (:nodes test)))]
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
  (fn gen [test ctx]
    (let [minority (random-minority (:nodes test))]
      ; TODO: This is what was written in the code, but I think it's actually
      ; wrong: we want to isolate *then* rando, right? -KRK 2023
      [{:type :info, :f :rando, :value minority}
       {:type :info, :f :isolate, :value minority}
       ; TODO: Is this supposed to sleep before recovery or after? -KRK 2023
       (gen/sleep (or recovery-delay 0))
       {:type :info, :f :recover, :value nil}])))

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
    (gen/nemesis {:type :info, :f :recover})
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
