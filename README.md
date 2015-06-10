## TNT4Spark
Track and Trace for Apache Spark. TNT4Spark provides an implementation of `SparkListener` for `SparkContext`.
TNT4Spark allows developers to track execution, measure performance and help with diagnostics of your Spark applications.

### Why TNT4Spark?
* Track and Trace Spark application execution @ runtime
* Measure execution of stages, jobs & tasks
* Detect and report task failures during execution
* Visualize your Spark application execution (via JESL and jkoolcloud integration)

### Using TNT4Spark
Using TNT4Spark is easy:
```java
...
SparkConf conf = new SparkConf().setAppName("my.spark.app");
JavaSparkContext sc = new JavaSparkContext(conf);

// add TNT4Spark listener to your spark context
sc.addSparkListener(new TNTSparkListener("my.spark.app"));
...
```
TNT4Spark uses TNT4J API to track job execution. Combining TNT4Spark with JESL (http://nastel.github.io/JESL/) lets developers stream data collected by TNT4Spark into a jKool -- real-time streaming and vizualization platform (see https://www.jkoolcloud.com). 

#### Add the following arguments to your java start-up
```
-Dtnt4j.config=<home>/config/tnt4j.properties -Dtnt4j.token.repository=<home>/config/tnt4j-tokens.properties 
```
To enable automatic application dump add the following arguments:
```
-Dtnt4j.dump.on.vm.shutdown=true -Dtnt4j.dump.on.exception=true -Dtnt4j.dump.provider.default=true 
```
Optionally you can add the following parameters to define default data center name and geo location:
```
-Dtnt4j.source.DATACENTER=YourDataCenterName -Dtnt4j.source.GEOADDR="Melville,NY" 
```

## Requirements
* JDK 1.8
* TNT4J (http://nastel.github.io/TNT4J/)
* Apache Spark 1.2.1 or higher (https://spark.apache.org/)
