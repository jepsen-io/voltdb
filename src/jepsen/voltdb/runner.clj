(ns jepsen.voltdb.runner
  "Runs VoltDB tests. Provides exit status reporting."
  (:gen-class)
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen.core :as jepsen]
            [jepsen.voltdb :as voltdb]
            [jepsen.voltdb [dirty-read :as dirty-read]
                           [multi      :as multi]
                           [single     :as single]]))

(defn one-of
  "Takes a collection and returns a string like \"Must be one of ...\" and a
  list of names. For maps, uses keys."
  [coll]
  (str "Must be one of "
       (pr-str (sort (map name (if (map? coll) (keys coll) coll))))))

(def tests
  "A map of test names to test constructors."
  {"multi"       multi/multi-test
   "single"      single/single-test
   "dirty-read"  dirty-read/dirty-read-test})

(def optspec
  "Command line options for tools.cli"
  [["-h" "--help" "Print out this message and exit"]

   ["-t" "--time-limit SECONDS"
    "Excluding setup and teardown, how long should a test run for, in seconds?"
    :default  150
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-c" "--test-count NUMBER"
    "How many times should we repeat a test?"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   [nil "--strong-reads" "Use stored procedure including a write for all reads"]

   [nil "--skip-os" "Don't perform OS setup"]

   ["-p" "--procedure-call-timeout MILLISECONDS"
    "How long should we wait before timing out procedure calls?"
    :default 1000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-u" "--tarball URL" "URL for the Mongo tarball to install. May be either HTTP, HTTPS, or a local file. For instance, --tarball https://foo.com/mongo.tgz, or file:///tmp/mongo.tgz"
    :validate [(partial re-find #"^(file|https?)://.*\.(tar)")
               "Must be a file://, http://, or https:// URL including .tar"]]])

(def usage
  (str "Usage: java -jar jepsen.voltdb.jar TEST-NAME [OPTIONS ...]

Runs a Jepsen test and exits with a status code:

  0     All tests passed
  1     Some test failed
  254   Invalid arguments
  255   Internal Jepsen error

Test names: " (str/join ", " (keys tests))
       "\n\nOptions:\n"))

(defn validate-test-name
  "Takes a tools.cli result map, and adds an error if the given
  arguments don't specify a valid test"
  [parsed]
  (if (= 1 (count (:arguments parsed)))
    parsed
    (update parsed :errors conj
            (str "No test name was provided. Use one of "
                 (str/join ", " (keys tests))))))

(defn validate-tarball
  "Ensures a tarball is present."
  [parsed]
  (if (:tarball (:options parsed))
    parsed
    (update parsed :errors conj "No tarball URL provided")))

(defn rename-keys
  "Given a map m, and a map of keys to replacement keys, yields m with keys
  renamed."
  [m replacements]
  (reduce (fn [m [k k']]
            (-> m
                (assoc k' (get m k))
                (dissoc k)))
          m
          replacements))


(defn -main
  [& args]
  (try
    (let [{:keys [options
                  arguments
                  summary
                  errors]} (-> args
                               (cli/parse-opts optspec)
                               validate-test-name
                               validate-tarball)
          options (rename-keys options {:strong-reads :strong-reads?
                                        :no-reads     :no-reads?
                                        :skip-os      :skip-os?})
          test-fn (get tests (first args))]

      ; Help?
      (when (:help options)
        (println usage)
        (println summary)
        (System/exit 0))

      ; Bad args?
      (when-not (empty? errors)
        (dorun (map println errors))
        (System/exit 254))

      (info "Test options:\n" (with-out-str (pprint options)))

      ; Run test
      (dotimes [i (:test-count options)]
        (when-not (:valid? (:results (jepsen/run! (test-fn options))))
          (System/exit 1)))

      (System/exit 0))
    (catch Throwable t
      (fatal t "Oh jeez, I'm sorry, Jepsen broke. Here's why:")
      (System/exit 255))))
