(ns jepsen.voltdb.redundant-register-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb.redundant-register :refer :all]
            [jepsen.core :as jepsen]))

(def tarball "http://downloads.voltdb.com/technologies/server/voltdb-ent-latest.tar.gz")

(deftest a-test
  (is (:valid? (:results (jepsen/run! (rregister-test tarball))))))
