# jepsen.voltdb

Jepsen tests for voltdb.

## Usage

```
lein run -- --help
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
