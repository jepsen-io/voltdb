(ns jepsen.voltdb.client
  "A wrapper around the VoltDB client library. Includes support functions for
  opening and closing clients, converting datatypes, handling errors, etc."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :as util])
  (:import (org.voltdb VoltTable
                       VoltType
                       VoltTableRow)
           (org.voltdb.client Client
                              ClientConfig
                              ClientFactory
                              ClientResponse
                              ProcedureCallback)))

(defn close!
  "Closes a client."
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

(defn kill-reconnect-threads!
  "VoltDB client leaks reconnect threads; this kills them all."
  []
  (doseq [t (keys (Thread/getAllStackTraces))]
    (when (= "Retry Connection" (.getName t))
      ; The reconnect loop swallows Exception so we can't even use interrupt
      ; here. Luckily I don't think it has too many locks we have to worry
      ; about.
      (.stop t))))

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
           ; TODO: is reconnection even something you can disable any more?
           ; https://docs.voltdb.com/javadoc/java-client-api/org/voltdb/client/ClientConfig.html
           ; makes it seem like maybe no? -KRK 2023
           ; (.setReconnectOnConnectionLoss (get opts :reconnect? true))
           ; We don't want to try and connect to all nodes
           (.setTopologyChangeAware false)
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
