(ns jepsen.voltdb.multi-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb-test :refer [tarball]]
            [jepsen.voltdb.multi :refer :all]
            [jepsen.core :as jepsen]))

(defn run [t]
  (is (:valid? (:results (jepsen/run! t)))))

(deftest a-test
  (loop []
    (when (run (multi-test {:tarball tarball
                            :procedure-call-timeout 5000
                            :time-limit 200}))
      (recur)
    )))
