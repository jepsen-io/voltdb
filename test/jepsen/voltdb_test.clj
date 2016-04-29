(ns jepsen.voltdb-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb :refer :all]
            [jepsen.core :as jepsen]))

(def tarball "http://downloads.voltdb.com/technologies/server/voltdb-ent-latest.tar.gz")

(deftest a-test
  (is (:valid? (:results (jepsen/run! (voltdb-test tarball))))))
