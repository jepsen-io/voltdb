(ns jepsen.voltdb
  (:require [jepsen [core         :as jepsen]
                    [db           :as db]
                    [control      :as c :refer [|]]
                    [checker      :as checker]
                    [client       :as client]
                    [generator    :as gen]
                    [independent  :as independent]
                    [nemesis      :as nemesis]
                    [tests        :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [knossos.model        :as model]
            [clojure.string       :as str]
            [clojure.data.xml     :as xml]
            [clojure.java.shell   :refer [sh]]
            [clojure.tools.logging :refer [info warn]])
  (:import (org.voltdb VoltTable
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse)))

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

(defn sql-cmd!
  "Takes an SQL query and runs it on the local node via sqlcmd"
  [query]
  (c/cd base-dir
        (c/sudo username
                (c/exec "bin/sqlcmd" (str "--query=" query)))))

(defn build-stored-procedures!
  "Compiles and packages stored procedures in procedures/"
  []
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


(defn install-stored-procedures!
  "Uploads stored procedures jar and loads it into VoltDB"
  []
  (c/upload "procedures/jepsen-procedures.jar"
            (str base-dir "/jepsen-procedures.jar"))
  (sql-cmd! "load classes jepsen-procedures.jar"))

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
        (await-initialization))

      ; Wait for convergence
      (jepsen/synchronize test)

      ; Stored procedures
      (when (= node (jepsen/primary test))
        (build-stored-procedures!)
        (install-stored-procedures!)))

    (teardown! [_ test node]
      (stop! node)
      (c/su))
;        (c/exec :rm :-rf (c/lit (str base-dir "/*")))))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/stdout.log")
       (str base-dir "/log/volt.log")])))

(defn connect
  "Opens a connection to the given node and returns a voltdb client."
  [node]
  (-> (doto (ClientConfig. "" "")
        (.setReconnectOnConnectionLoss true))
        (ClientFactory/createClient)
        (doto
          (.createConnection (name node)))))

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
                   (aset cols j (.get row j (nth column-types j)))
                   (recur (inc j))))
               (recur (conj! rows (clojure.lang.PersistentStructMap/construct
                                    basis
                                    (seq cols)))))
             ; Done
             (persistent! rows)))}))

(defn call!
  "Call a stored procedure and returns a seq of VoltTable results."
  [client procedure & args]
  (let [res (.callProcedure client procedure (into-array Object args))]
    ; Docs claim callProcedure will throw, but tutorial checks anyway so ???
    (assert (= (.getStatus res) ClientResponse/SUCCESS))
    (map volt-table->map (.getResults res))))

(defn ad-hoc!
  "Run an ad-hoc SQL stored procedure."
  [client & args]
  (apply call! client "@AdHoc" args))

(defn client
  "A single-register client."
  ([] (client nil))
  ([conn]
   (let [initialized? (promise)]
     (reify client/Client
       (setup! [_ test node]
         (let [conn (connect node)]
           (c/on node
                 (when (deliver initialized? true)
                   ; Create table
                   (sql-cmd! "CREATE TABLE registers (
                             id          INTEGER UNIQUE NOT NULL,
                             value       INTEGER NOT NULL,
                             PRIMARY KEY (id)
                             );
                             PARTITION TABLE registers ON COLUMN id;")
                   (info node "table created")))
           (client conn)))

       (invoke! [this test op]
         (let [id     (key (:value op))
               value  (val (:value op))]
           (case (:f op)
             :read   (let [v (-> conn
                                 (call! "REGISTERS.select" id)
                                 first
                                 :rows
                                 first
                                 :VALUE)]
                           (assoc op
                                  :type :ok
                                  :value (independent/tuple id v)))
             :write (do (call! conn "REGISTERS.upsert" id value)
                        (assoc op :type :ok))
             :cas   (let [[v v'] value
                          res (-> conn
                                  (ad-hoc! "UPDATE registers SET value = ?
                                           WHERE id = ? AND value = ?"
                                           v' id v)
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

(defn voltdb-test
  "Takes a tarball URL"
  [url]
  (assoc tests/noop-test
         :name    "voltdb"
         :os      debian/os
         :client  (client)
         :db      (db url)
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
                         (gen/time-limit 200))))
