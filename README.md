# jepsen.voltdb

A Clojure library designed to ... well, that part is up to you.

You'll need to snarf voltdb/voltdb-6.2.1.jar (or whatever version you're using) from a node and drop it in procedures/.


```sh
scp n1:/opt/voltdb/voltdb/voltdb-6.2.1.jar ./
scp n1:/opt/voltdb/voltdb/voltdbclient-6.2.1.jar ./
```

Jepsen will run this automatically and copy the jarfile back onto the cluster

```
javac -classpath "./:./*" -d ./obj *.java
jar cvf jepsen-procedures.jar -C obj .
```

## Usage

FIXME

## License

Copyright © 2016 FIXME

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
