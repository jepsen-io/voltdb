(ns jepsen.voltdb-test
  (:require [clojure.test :refer :all]
            [clojure.string :as str]
            [jepsen.voltdb :refer :all]
            [jepsen.core :as jepsen]))

(def tarball (str/trim (slurp "tarball.url")))

(when (str/blank? tarball)
  (println "Please put the URL to a VoltDB enterprise tarball into a file named `tarball.url`.")
  (System/exit 255))
