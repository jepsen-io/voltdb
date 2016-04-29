(ns jepsen.voltdb
  (:require [jepsen [core       :as jepsen]
                    [db         :as db]
                    [control    :as c :refer [|]]
                    [tests      :as tests]]
            [jepsen.os.debian     :as debian]
            [jepsen.control.util  :as cu]
            [clojure.string :as str]
            [clojure.data.xml :as xml]
            [clojure.tools.logging :refer [info warn]]))

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
                      :--host (jepsen/primary test))))

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
        (start! test)))

    (teardown! [_ test node]
      (c/su
;        (c/exec :rm :-rf (c/lit (str base-dir "/*")))))))
    ))

    db/LogFiles
    (log-files [db test node]
      [(str base-dir "/stdout.log")
       (str base-dir "/log/volt.log")])))

(defn voltdb-test
  "Takes a tarball URL"
  [url]
  (assoc tests/noop-test
         :name "voltdb"
         :os   debian/os
         :db   (db url)))
