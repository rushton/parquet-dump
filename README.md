# Parquet Dump

Converts binary parquet data from a pipe into JSON outputted to STDOUT

# Build

```
sbt assembly
```

# Run

```
cat /tmp/myparquet/*.parquet | java -jar target/scala-2.11/Parquet-Dump-assembly-1.0.0.jar
```
