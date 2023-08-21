(defproject jepsen.voltdb "0.1.0-SNAPSHOT"
  :description "Jepsen VoltDB tests"
  :url "https://github.com/jepsen-io/voltdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.11.1"]
                 [jepsen "0.3.3"]
                 [org.clojure/data.xml "0.0.8"]
                 [org.voltdb/voltdbclient "6.2"]]
  :jvm-opts ["-Xmx8g"
             "-XX:MaxInlineLevel=32"
             "-server"]
  :main jepsen.voltdb.runner
  :aot  [jepsen.voltdb.runner
         clojure.tools.logging.impl])
