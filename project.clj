(defproject jepsen.voltdb "0.1.0-SNAPSHOT"
  :description "Jepsen VoltDB tests"
    :url "https://github.com/aphyr/jepsen.voltdb"
  :license nil
  ;{:name "Eclipse Public License"
  ; :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.1-SNAPSHOT"]
                 [knossos "0.2.8-SNAPSHOT"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.voltdb/voltdbclient "6.2"]])
