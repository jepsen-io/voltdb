(ns jepsen.voltdb
  "OS and database setup functions, plus some older, currently unused nemeses
  that should be ported over to voltdb.nemesis."
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
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [jepsen.control.net   :as cn]
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
(def export-csv-file "What file do we export CSV data to?"
  (str base-dir "/export.csv"))

(defn os
  "Given OS, plus python & jdk"
  [os]
  (reify os/OS
    (setup! [_ test node]
      (os/setup! os test node)
      (debian/install ["python3" "openjdk-17-jdk-headless"])
      (c/exec :update-alternatives :--install "/usr/bin/python" "python"
              "/usr/bin/python3" 1))

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
                ; TODO: Make k configurable
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
      [:frequency {:time 2}]] ; milliseconds
     ; Not exactly sure what these do! Adapted from ghostbuster -- KRK 2023
     [:systemsettings
      [:flushinterval {:minimum 10}
       [:export {:interval 10}]]]
     ; Export configuration, for export tests
     [:export
      [:configuration {:enabled true, :target "export_target", :type "file"}
       [:property {:name "type"} "csv"]
       [:property {:name "nonce"} "export_target_2"]
       [:property {:name "outdir"} export-csv-file]]]])))

(defn init-db!
  "run voltdb init"
  [node]
  (info "Initializing voltdb")
  (c/sudo username
          (c/cd base-dir
              ; We think there's a bug that breaks sqlcmd if it runs early in
              ; the creation of a fresh DB--it'll log "Cannot invoke
              ; java.util.Map.values() because arglists is null". To work
              ; around that, we're creating a table so the schema is nonempty.
                (let [init-schema "CREATE TABLE work_around_volt_bug (
                                 id int not null
                                 );"
                      init-schema-file "init-schema"]
                  (cu/write-file! init-schema init-schema-file)
                  (c/exec (str base-dir "/bin/voltdb")
                          :init
                          :-s init-schema-file
                          :--config (str base-dir "/deployment.xml")
                          | :tee (str base-dir "/log/stdout.log")))))
  (info node "initialized"))

(defn configure!
  "Prepares config files and creates fresh DB."
  [test node]
  (c/sudo username
          (c/cd base-dir
                (c/upload (:license test) (str base-dir "/license.xml"))
                (cu/write-file! (deployment-xml test) "deployment.xml")
                (init-db! node)
                (c/exec :ln :-f :-s (str base-dir "/voltdbroot/log/volt.log") (str base-dir "/log/volt.log")))))

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
  (cu/await-tcp-port client-port {:log-interval 30000
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
  "Starts the VoltDB daemon."
  [test]
  (c/sudo username
          (c/cd base-dir
                (info "Starting voltdb")
                (cu/start-daemon! {:logfile (str base-dir "/log/stdout.log")
                                   :pidfile (str base-dir "/pidfile")
                                   :chdir   base-dir}
                                  (str base-dir "/bin/voltdb")
                                  :start
                                  :--count (count (:nodes test))
                                  :--host (->> (:nodes test)
                                               (map cn/ip)
                                               (str/join ","))))))

(defn recover!
  "Restarts all nodes in the test."
  [test]
  (c/on-nodes test (partial db/start! (:db test))))

(defn rejoin!
  "Rejoins a voltdb node. Serialized to work around a bug in voltdb where
  multiple rejoins can take down cluster nodes."
  [test node]
  ; This bug has been fixed, so we probably don't need to lock here - KRK 2023
  (locking rejoin!
    (info "rejoining" node)
    (db/start! (:db test) test node)
    (await-rejoin node)))

(defn stop-recover!
  "Stops all nodes, then recovers all nodes. Useful when Volt's lost majority
  and nodes kill themselves."
  ([test]
   (c/on-nodes test (partial db/kill! (:db test)))
   (recover! test)))

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
    (setup! [this test node]
      ; Download and unpack
      (install! node url force-download?)

      ; Prepare stored procedures in parallel
      (let [procedures (future (when (= node (jepsen/primary test))
                                 (snarf-procedure-deps!)
                                 (build-stored-procedures!)
                                 (upload-stored-procedures! node)))]
        ; Boot
        (configure! test node)
        (db/start! this test node)
        (await-start node)

        ; Wait for convergence
        (jepsen/synchronize test 240)

        ; Finish procedures
        @procedures
        (when (= node (jepsen/primary test))
          (load-stored-procedures! node))))

    (teardown! [this test node]
      (db/kill! this test node)
      (c/su
       (c/exec :rm :-rf (c/lit (str base-dir "/*"))))
      (vc/kill-reconnect-threads!))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/log/stdout.log")
       (str base-dir "/log/volt.log")
       (str base-dir "/deployment.xml")])

    db/Kill
    (kill! [this test node]
      (c/su
        (cu/stop-daemon! (str base-dir "/pidfile"))))

    (start! [this test node]
      (start-daemon! test))

    db/Pause
    (pause! [this test node]
      ; TODO: target volt specifically
      (c/su (cu/grepkill! :stop "java")))

    (resume! [this test node]
      ; TODO: target volt specifically
      (c/su (cu/grepkill! :cont "java")))))
