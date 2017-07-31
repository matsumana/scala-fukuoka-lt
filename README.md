# How to build

```
$ ./mvnw package
```

# How to submit job

```
$ /path/to/flink_home/bin/flink run -d -c info.matsumana.flink.TweetCollect ./target/demo-0.1.0-SNAPSHOT.jar ./paramsTweetCollect.properties
$ /path/to/flink_home/bin/flink run -d -c info.matsumana.flink.TweetAggregate ./target/demo-0.1.0-SNAPSHOT.jar ./paramsTweetAggregate.properties
```
