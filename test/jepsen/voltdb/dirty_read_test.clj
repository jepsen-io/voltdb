(ns jepsen.voltdb.dirty-read-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb.dirty-read :refer :all]
            [jepsen.voltdb-test :refer [tarball]]
            [jepsen.core :as jepsen]))

(deftest a-test
  (loop []
    (when (is (:valid? (:results (jepsen/run! (dirty-read-test {:tarball tarball
    :procedure-call-timeout 5000}
)))))
      (recur))))
