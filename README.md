# Parquet Dumper

Dumps parquet as json to stdout from a pipe

# Build

```
sbt assembly
```

# Run

```
cat /tmp/myparquet/*.parquet | java -jar target/scala-2.11/Parquet-Dump-assembly-1.0.0.jar
```
