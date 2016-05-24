(ns jepsen.voltdb.redundant-register-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb.redundant-register :refer :all]
            [jepsen.voltdb-test :refer [tarball]]
            [jepsen.core :as jepsen]))

(deftest a-test
  (is (:valid? (:results (jepsen/run! (rregister-test tarball))))))
