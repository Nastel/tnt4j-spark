# TNT4Spark
Track and Trace for Apache Spark. TNT4Spark provides an implementation of `SparkListener` for `SparkContext`.
TNT4Spark allows developers to track execution, measure performance and help with diagnostics of your Spark applications.

Using TNT4Spark is easy:
```java
...
SparkConf conf = new SparkConf().setAppName("my.app");
JavaSparkContext sc = new JavaSparkContext(conf);

// add TNT4Spark listener to your spark context
sc.addSparkListener(new TNTSparkListener("my.app"));
...
```
TNT4Spark uses TNT4J API to track job execution. Combining TNT4Spark witj JESL (http://nastel.github.io/JESL/) lets developers stream data collected by TNT4Spark into a jKool -- real-time streaming and vizualization platform (see https://www.jkoolcloud.com). 

# Requirements
* JDK 1.8
* TNT4J (http://nastel.github.io/TNT4J/)
* Apache Spark 1.2.1 or higher (https://spark.apache.org/)
