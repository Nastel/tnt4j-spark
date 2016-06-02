## About TNT4J-Spark
Track and Trace for Apache Spark. TNT4J-Spark provides an implementation of `SparkListener` for `SparkContext`.
TNT4J-Spark allows developers to track execution, measure performance and help with diagnostics of your Spark applications.

### Why TNT4J-Spark?
* Track and Trace Spark application execution @ runtime
* Measure performance & execution of stages, jobs, tasks
* Detect and report task failures during execution
* Visualize your Spark application execution (via JESL and jkoolcloud integration)

NOTE: See https://www.jkoolcloud.com and (JESL) http://nastel.github.io/JESL/ to vizualize Spark application execution.

### Using TNT4J-Spark
TNT4J-Spark is easy, just include a few lines into your application:
```java
...
SparkConf conf = new SparkConf().setAppName("my.spark.app");
JavaSparkContext sc = new JavaSparkContext(conf);

// add TNT4J-Spark listener to your spark context
sc.addSparkListener(new TNTSparkListener("my.spark.app"));
...
```
Make sure you edit `config/tnt4j.properties` and specify TNT4J configuration for your application `my.spark.app`.
```
; Stanza used for Spark Applications
; replace `my.spark.app` with the name used when creating TNTSparkListener
{
	source: my.spark.app
	source.factory: com.jkoolcloud.tnt4j.source.SourceFactoryImpl
	source.factory.GEOADDR: NewYork
	source.factory.DATACENTER: HQDC
	source.factory.RootFQN: SERVER=?#DATACENTER=?#GEOADDR=?	
	
	tracker.factory: com.jkoolcloud.tnt4j.tracker.DefaultTrackerFactory
	dump.sink.factory: com.jkoolcloud.tnt4j.dump.DefaultDumpSinkFactory
	event.sink.factory: com.jkoolcloud.tnt4j.sink.FileEventSinkFactory

	; Configure default sink filter based on level and time (elapsed/wait)
	event.sink.factory.Filter: com.jkoolcloud.tnt4j.filters.EventLevelTimeFilter
	event.sink.factory.Filter.Level: TRACE
	; Uncomment lines below to filter out events based on elapsed time and wait time
	; Timed event/activities greater or equal to given values will be logged
	;event.sink.factory.Filter.ElapsedUsec: 100
	;event.sink.factory.Filter.WaitUsec: 100
	
	event.formatter: com.jkoolcloud.tnt4j.format.SimpleFormatter
	tracking.selector: com.jkoolcloud.tnt4j.selector.DefaultTrackingSelector
	tracking.selector.Repository: com.jkoolcloud.tnt4j.repository.FileTokenRepository
}
```
TNT4J-Spark uses TNT4J API to track job execution. Combining TNT4J-Spark with JESL (http://nastel.github.io/JESL/) lets developers stream data collected by TNT4J-Spark into jKool Cloud -- real-time streaming and vizualization platform (see https://www.jkoolcloud.com). 

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
