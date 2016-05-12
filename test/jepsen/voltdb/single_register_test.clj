(ns jepsen.voltdb.single-register-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb.single-register :refer :all]
            [jepsen.core :as jepsen]))

(def tarball "http://downloads.voltdb.com/technologies/server/voltdb-ent-latest.tar.gz")

(deftest a-test
  (is (:valid? (:results (jepsen/run! (single-register-test tarball))))))
