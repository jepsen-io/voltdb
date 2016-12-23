(ns jepsen.voltdb.single-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb-test :refer [tarball]]
            [jepsen.voltdb.single :refer :all]
            [jepsen.core :as jepsen]))

(defn run [t]
  (is (:valid? (:results (jepsen/run! t)))))

(deftest normal-reads-test
  (loop []
    (when (run (single-test {:tarball tarball
                             :strong-reads? false
                             :procedure-call-timeout 45000
                             :time-limit 300}))
      (recur))))

(deftest strong-reads-test
  (loop []
    (when (run (single-test {:tarball tarball
                             :strong-reads? true
                             :procedure-call-timeout 5000
                             :time-limit 50}))
      (recur))))

(deftest no-reads-test
  (loop []
    (when (run (single-test {:tarball tarball
                             :no-reads? true
                             :procedure-call-timeout 5000
                             :time-limit 50}))
      (recur))))
