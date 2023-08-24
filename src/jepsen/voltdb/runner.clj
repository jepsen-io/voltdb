(ns jepsen.voltdb.runner
  "Runs VoltDB tests from the command line."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [core :as jepsen]
                    [cli :as cli]]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [dirty-read :as dirty-read]
                           [multi      :as multi]
                           [single     :as single]
                           [redundant-register :as redundant-register]]))

(def workloads
  "A map of workload names names to functions that take CLI options and return
  workload maps"
  {:multi              multi/multi-test
   :single             single/single-test
   :dirty-read         dirty-read/dirty-read-test
   :redundant-register redundant-register/rregister-test})

(def opt-spec
  "Command line options for tools.cli"
   [["-l" "--license FILE" "Path to the VoltDB license file on the control node"
    :default "license.xml"]

   [nil "--recovery-delay SECONDS"
    "How long should we wait before killing nodes and recovering?"
    :default 0
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   ["-r" "--rate HZ" "Approximate number of requests per second. Note: many workloads currently hardcode their request rate."
    :default 100
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "must be a positive number"]]

   [nil "--strong-reads" "Use stored procedure including a write for all reads"]

   [nil "--skip-os" "Don't perform OS setup"]

   [nil "--force-download" "Re-download tarballs, even if cached locally"]

   ["-p" "--procedure-call-timeout MILLISECONDS"
    "How long should we wait before timing out procedure calls?"
    :default 1000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-u" "--tarball URL" "URL for the VoltDB tarball to install. May be either HTTP, HTTPS, or a local file on this control node. For instance, --tarball https://foo.com/voltdb-ent.tar.gz, or file://voltdb-ent.tar.gz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar)")
               "Must be a file://, http://, or https:// URL including .tar"]]

   ["-w" "--workload NAME" "What workload should we run?"
    :default :single
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]])

(defn voltdb-test
  "Takes parsed CLI options from -main and constructs a Jepsen test map."
  [opts]
  (let [workload-name (:workload opts)
        ; Right now workloads construct entire test maps. We'll refactor this
        ; in the next commit.
        workload ((workloads workload-name) opts)]
    workload))

(defn -main
  "Main entry point for the CLI. Takes CLI options and runs tests, launches a
  web server, analyzes results, etc."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn voltdb-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
