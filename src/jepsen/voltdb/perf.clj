(ns jepsen.voltdb.perf
  "Performance benchmarks for single and multi transactions, with a mixed
  read-write fixed-concurrency workload, over varying node count. This is
  currently marooned--it was an experiment during the 2016 Jepsen tests, but
  isn't hooked up to the CLI."
  (:require [jepsen [core           :as jepsen]
                    [control        :as c :refer [|]]
                    [checker        :as checker]
                    [client         :as client]
                    [generator      :as gen]
                    [independent    :as independent]
                    [nemesis        :as nemesis]
                    [os             :as os]
                    [tests          :as tests]]
            [jepsen.os.debian       :as debian]
            [jepsen.checker.timeline :as timeline]
            [jepsen.voltdb          :as voltdb]
            [jepsen.voltdb.multi    :as multi]
            [jepsen.voltdb.single   :as single]
            [knossos.model          :as model]
            [knossos.op             :as op]
            [clojure.string         :as str]
            [clojure.pprint         :refer [pprint]]
            [clojure.core.reducers  :as r]
            [clojure.tools.logging  :refer [info warn]]))

(defn single-perf-test
  "Special options, in addition to voltdb/base-test:

      :strong-reads                 Whether to perform normal or strong selects
      :no-reads                     Don't bother with reads at all
      :procedure-call-timeout       How long in ms to wait for proc calls
      :connection-response-timeout  How long in ms to wait for connections"
  [opts]
  (voltdb/base-test
    (assoc opts
           :name    (str "voltdb perf single " (count (:nodes opts)))
           :client  (single/client (select-keys opts [:strong-reads
                                                      :procedure-call-timeout
                                                      :connection-response-timeout]))
           :model   (model/cas-register nil)
           :checker (checker/perf)
           :concurrency 160
           :generator (->> (independent/concurrent-generator
                             20
                             (range)
                             (fn [id]
                               (->> (gen/mix [single/r
                                              single/r
                                              single/w
                                              single/cas])
                                    (gen/time-limit 60))))
                           (voltdb/general-gen opts)))))

(defn multi-perf-test
  "Special options, in addition to voltdb/base-test:

      :procedure-call-timeout       How long in ms to wait for proc calls
      :connection-response-timeout  How long in ms to wait for connections"
  [opts]
  (let [ks [:x :y]
        system-count 1000]
    (voltdb/base-test
      (assoc opts
             :name    (str "voltdb perf multi " (count (:nodes opts)))
             :client  (multi/client
                        (merge
                          {:keys         ks
                           :system-count system-count}
                          (select-keys opts
                                       [:keys
                                        :system-count
                                        :procedure-call-timeout
                                        :connection-response-timeout])))
             :model   (model/multi-register (zipmap ks (repeat 0)))
             :checker (checker/perf)
             :concurrency 16
             :generator
             (->> (independent/concurrent-generator
                    2
                    (range)
                    (fn [id]
                      (->> (multi/txn-gen ks)
                           (gen/time-limit 60))))
                  (voltdb/general-gen opts))))))
