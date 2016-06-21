(ns jepsen.voltdb.perf-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb-test :refer [tarball]]
            [jepsen.voltdb.perf :refer :all]
            [jepsen.core :as jepsen]
            [clojure.pprint :refer [pprint]]))

(defn run! [t]
  (let [res (:results (jepsen/run! t))]
    (or (is (:valid? res))
        (pprint res)
        (println (:error res)))))

(deftest a-test
  (let [nodes [:n1 :n2 :n3 :n4 :n5 :n6 :n7 :n8 :n9 :10]]
    (doseq [i [8 1 4 2]
            t [single-perf-test multi-perf-test]]
      (run! (t {:time-limit 100
                :tarball "http://voltdb.com/downloads/technologies/server/LINUX-voltdb-ent-6.4.jepsen4.tar.gz"
                :procedure-call-timeout 30000
                :nodes (take i nodes)})))))
