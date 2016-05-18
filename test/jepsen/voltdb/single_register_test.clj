(ns jepsen.voltdb.single-register-test
  (:require [clojure.test :refer :all]
            [jepsen.voltdb.single-register :refer :all]
            [jepsen.core :as jepsen]))

(def tarball "http://downloads.voltdb.com/technologies/server/voltdb-ent-latest.tar.gz")

(defn run [t]
  (is (:valid? (:results (jepsen/run! t)))))

(deftest normal-read-test
  (run (single-register-test {:tarball tarball
                              :strong-read? false
                              :procedure-call-timeout 45000
                              :time-limit 60})))

(deftest strong-read-test
  (run (single-register-test {:tarball tarball
                              :strong-read? true
                              :procedure-call-timeout 1000
                              :time-limit 100})))
