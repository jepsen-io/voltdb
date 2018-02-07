(defproject jepsen.voltdb "0.1.0-SNAPSHOT"
  :description "Jepsen VoltDB tests"
  :url "https://github.com/jepsen-io/voltdb"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.3.3"]
                 [jepsen "0.1.2"]
                 [org.clojure/data.xml "0.0.8"]]
                 ;[org.voltdb/voltdbclient "6.2"]]
  :jvm-opts ["-Xmx8g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-server"]
  :resource-paths ["../voltdb/voltdb/voltdb-8.0.jar" "../voltdb/voltdb/voltdbclient-8.0.jar"]
  :main jepsen.voltdb.runner
  :aot  [jepsen.voltdb.runner
         clojure.tools.logging.impl])
