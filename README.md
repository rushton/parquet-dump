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

# Build

```
sbt assembly
```

# Run

```
cat /tmp/myparquet/*.parquet | java -jar target/scala-2.11/Parquet-Dump-assembly-1.0.0.jar
```
