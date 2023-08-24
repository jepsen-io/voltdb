(ns jepsen.voltdb.runner
  "Runs VoltDB tests from the command line."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [jepsen [core :as jepsen]
                    [checker :as checker]
                    [cli :as cli]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.os.debian :as debian]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [dirty-read :as dirty-read]
                           [multi      :as multi]
                           [nemesis    :as nemesis]
                           [single     :as single]
                           [redundant-register :as redundant-register]]))

(def workloads
  "A map of workload names names to functions that take CLI options and return
  workload maps"
  {:dirty-read         dirty-read/workload
   :multi              multi/workload
   :redundant-register redundant-register/workload
   :single             single/workload})

(def nemeses
  "All nemesis faults we know about."
  ; TODO: add pause, kill, rando, bitflip/truncate disk files, ...
  #{:partition :clock})

(def special-nemeses
  "A map of special nemesis names to collections of faults."
  {:none []
   :all  [:partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def opt-spec
  "Command line options for tools.cli"
   [["-l" "--license FILE" "Path to the VoltDB license file on the control node"
    :default "license.xml"]

   [nil "--recovery-delay SECONDS"
    "How long should we wait before killing nodes and recovering?"
    :default 0
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--nemesis FAULTS" "A comma-separated list of faults to inject."
    :parse-fn parse-nemesis-spec
    :validate [(partial every? (fn [nem]
                                 (or (nemeses nem)
                                     (special-nemeses nem))))
               (cli/one-of (concat nemeses (keys special-nemeses)))]]

   [nil "--nemesis-interval SECONDS" "How long between nemesis operations, on average, for each class of fault?"
    :default  10
    :parse-fn read-string
    :validate [pos? "must be positive"]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   ["-r" "--rate HZ" "Approximate number of requests per second, total"
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
        workload ((workloads workload-name) opts)
        db       (voltdb/db (:tarball opts) (:force-download opts))
        nemesis (nemesis/nemesis-package
                  {:db        db
                   :nodes     (:nodes test)
                   :faults    (:nemesis opts)
                   ; TODO: add support for targeting primaries
                   :partition {:targets [:majority :majorities-ring]}
                   :pause     {:targets [:one :majority :all]}
                   :kill      {:targets [:one :majority :all]}
                   :interval  (:nemesis-interval opts)})
        gen (->> (:generator workload)
                 (gen/stagger (/ (:rate opts)))
                 (gen/nemesis
                   [(gen/sleep 5)
                    (:generator nemesis)])
                 (gen/time-limit (:time-limit opts)))
        ; Is there a final generator for this workload?
        gen (if-let [final (:final-generator workload)]
              (gen/phases gen
                          ; Recovery
                          (gen/log "Recovering cluster")
                          (gen/nemesis (:final-generator nemesis))
                          (gen/log "Waiting for recovery")
                          (gen/sleep 10)
                          ; Final generators
                          (gen/clients final))
              ; No final generator
              gen)]
    (merge tests/noop-test
           opts
           {:name (str (name workload-name)
                       " " (str/join "," (map name (:nemesis opts))))
            :os        (if (:skip-os opts)
                         os/noop
                         (voltdb/os debian/os))
            :generator gen
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :db        db
            :checker   (checker/compose
                         {:perf       (checker/perf {:nemeses (:perf nemesis)})
                          :clock      (checker/clock-plot)
                          :stats      (checker/stats)
                          :exceptions (checker/unhandled-exceptions)
                          :workload   (:checker workload)})})))

(defn -main
  "Main entry point for the CLI. Takes CLI options and runs tests, launches a
  web server, analyzes results, etc."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn voltdb-test
                                         :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
