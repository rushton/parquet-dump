[![Build Status](https://travis-ci.org/tuneinc/parquet-dump.svg?branch=master)](https://travis-ci.org/tuneinc/parquet-dump)
# Parquet Dump

Converts binary parquet data from a pipe into JSON outputted to STDOUT

```
#> cat /tmp/foobar/*.parquet | java -jar target/scala-2.11/Parquet-Dump-assembly-1.0.0.jar
{"a":1,"b":null}
{"a":1,"b":null}
{"a":1,"b":null}
{"a":1,"b":null}
{"a":1,"b":null}
...
```

counting records per block
```
cat /tmp/foobar/*.parquet | java -jar target/scala-2.11/Parquet-Dump-assembly-1.0.0.jar
5
10
15
```

# Install

Download the latest jar from the [releases](../../releases/latest) page

# Build

```
sbt assembly
```

# Run

```
cat /tmp/myparquet/*.parquet | java -jar Parquet-Dump-assembly-1.0.0.jar
```
