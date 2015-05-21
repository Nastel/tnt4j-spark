# TNT4Spark
Track and Trace for Apache Spark. TNT4Spark provides an implementation of `SparkListener` for `SparkContext`.
Using TNT4Spark is easy:
```java
...
SparkConf conf = new SparkConf().setAppName("my.app");
JavaSparkContext sc = new JavaSparkContext(conf);

// add TNT4Spark listener to your spark context
sc.addSparkListener(new TNTSparkListener("my.app"));
...
```

# Requirements
* JDK 1.8
* TNT4J (http://nastel.github.io/TNT4J/)
* Apache Spark 1.2.1 or higher (https://spark.apache.org/)
