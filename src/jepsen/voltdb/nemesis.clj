(ns jepsen.voltdb.nemesis
  "Various packages of faults we can inject against VoltDB clusters."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [control :as c]
             [nemesis :as n]
             [generator :as gen]
             [net :as net]
             [util :as util]]
            [jepsen.control [util :as cu]]
            [jepsen.nemesis [combined :as nc]
             [time :as nt]]
            [slingshot.slingshot :refer [try+ throw+]]))

(defn nemesis-package
  "Takes nemesis options (see jepsen.nemesis.combined for details and examples)
  and constructs a package of nemesis and generators."
  [opts]
  (let [opts (update opts :faults set)]
    (-> (nc/nemesis-packages opts)
        ; TODO: custom nemeses--port these over from jepsen.voltdb.
        (concat [])
        (->> (remove nil?))
        nc/compose-packages)))
