(ns jepsen.voltdb.dirty-read-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb.dirty-read :refer :all]
            [jepsen.core :as jepsen]))

(def tarball "http://downloads.voltdb.com/technologies/server/voltdb-ent-latest.tar.gz")

(deftest a-test
  (is (:valid? (:results (jepsen/run! (dirty-read-test tarball))))))
