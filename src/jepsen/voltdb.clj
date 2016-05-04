(ns jepsen.voltdb
  (:require [jepsen [core       :as jepsen]
                    [db         :as db]
                    [control    :as c :refer [|]]
                    [client     :as client]
                    [tests      :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [clojure.string :as str]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :refer [info warn]])
  (:import (org.voltdb.client Client
                              ClientConfig
                              ClientFactory)))

(def username "voltdb")
(def base-dir "/opt/voltdb")

(defn install!
  "Install the given tarball URL"
  [node url]
  (c/su
    (debian/install-jdk8!)
    (info "JDK8 installed")
    (cu/install-tarball! node url base-dir)
    (cu/ensure-user! username)
    (c/exec :chown :-R (str username ":" username) base-dir)
    (info "RethinkDB unpacked")))

(defn deployment-xml
  "Generate a deployment.xml string for the given test."
  [test]
  (xml/emit-str
    (xml/sexp-as-element
      [:deployment {}
       [:cluster {:hostcount (count (:nodes test))
                  :kfactor (:k-factor test (dec (count (:nodes test))))}]
       [:paths {}
        [:voltdbroot {:path base-dir}]]])))

(defn configure!
  "Prepares config files and creates fresh DB."
  [node test]
  (c/sudo username
        (c/cd base-dir
              (c/exec :echo (deployment-xml test) :> "deployment.xml"))))

(defn await-initialization
  "Blocks until the logfile reports 'Server completed initialization'."
  [node]
  (info "Waiting for" node "to initialize")
  (c/cd base-dir
        (c/exec :tail :-f "stdout.log"
                | :grep :-m 1 "completed initialization"
                | :xargs (c/lit "echo \"\" >> stdout.log \\;")))
  (info node "initialized"))

(defn start!
  "Starts voltdb"
  [node test]
  (c/sudo username
    (cu/start-daemon! {:logfile (str base-dir "/stdout.log")
                       :pidfile (str base-dir "/pidfile")
                       :chdir   base-dir}
                      (str base-dir "/bin/voltdb")
                      :create
                      :--deployment (str base-dir "/deployment.xml")
                      :--host (jepsen/primary test))
    (Thread/sleep 5000)))

(defn stop!
  "Stops voltdb"
  [node]
  (c/su
    (cu/stop-daemon! (str base-dir "/pidfile"))))

(defn db
  "VoltDB around the given package tarball URL"
  [url]
  (reify db/DB
    (setup! [_ test node]
      (doto node
        (install! url)
        (configure! test)
        (start! test)
        (await-initialization)))

    (teardown! [_ test node]
      (stop! node)
      (c/su))
;        (c/exec :rm :-rf (c/lit (str base-dir "/*")))))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/stdout.log")
       (str base-dir "/log/volt.log")])))

(defn sql-cmd!
  "Takes an SQL query and runs it on the local node via sqlcmd"
  [query]
  (c/cd base-dir
        (c/sudo username
                (c/exec "bin/sqlcmd" (str "--query=" query)))))

(defn connect
  "Opens a connection to the given node and returns a voltdb client."
  [node]
  (-> (doto (ClientConfig. "" "")
        (.setReconnectOnConnectionLoss true))
      (ClientFactory/createClient)
      (doto
        (.createConnection (name node)))))

(defn client
  "A single-register client."
  ([] (client nil))
  ([conn]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [_ test node]
         (c/on node
               (try
                 ; Create table
                 (sql-cmd! "CREATE TABLE registers (
                           id          INTEGER UNIQUE NOT NULL,
                           value       INTEGER NOT NULL,
                           PRIMARY KEY (id)
                           );
                           PARTITION TABLE registers ON COLUMN id;")
                 (info node "table created")

                 (catch RuntimeException e
                   ; No IF NOT EXISTS clause for table creation
                   (when-not (re-find #"already exists" (.getMessage e))
                     (throw e))))

               ; Install stored procedures
               (when (deliver initialized? true)
                 (c/upload "procedures/jepsen-procedures.jar"
                           (str base-dir "/jepsen-procedures.jar"))
                 (sql-cmd! "load classes jepsen-procedures.jar")
                 (sql-cmd! "CREATE PROCEDURE FROM CLASS jepsen.procedures.ReadAll")))

         (client (connect node)))

       (invoke! [this test op])

       (teardown! [_ test]
         (.close conn))))))

(defn voltdb-test
  "Takes a tarball URL"
  [url]
  (assoc tests/noop-test
         :name    "voltdb"
         :os      debian/os
         :client  (client)
         :db      (db url)))
