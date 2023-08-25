# jepsen.voltdb

Jepsen tests for voltdb.

## Quickstart

You'll need a VoltDB tarball to install, either as a local file or an HTTP URL.
You'll also need a license file in this directory, by default called `license.xml`.

To run a single-key linearizability test with network partitions:

```
lein run test --tarball file://voltdb-ent-12.3.1.tar.gz -w single --nemesis partition
```

To run a full suite of tests with different workloads and nemeses:

```
lein run test-all --tarball file://voltdb-ent-12.3.1.tar.gz
```

To re-analyze a specific test with the current code:

```
lein run analyze store/whatever/some-timestamp/test.jepsen
```

To run a web server for browsing results in store/:

```
lein run serve
```

To build a fat jar that does all of the above:

```
lein uberjar
```

## Usage

```
lein run test --help
```

## Running

You may need to disable transparent huge pages on DB nodes (or, if running on LXC, on the host OS):

```
sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"
sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/defrag"
```

## License

Copyright © 2016 Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

## jepsen maven repo
https://mvnrepository.com/artifact/jepsen/jepsen?repo=clojars

## jepsen github
https://github.com/jepsen-io/voltdb

## clojure repo
https://clojure.org/releases/downloads_older
