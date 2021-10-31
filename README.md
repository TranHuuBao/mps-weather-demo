# Demo Weather Streaming for MPS (VNG)

## Enviroment
- Kafka
- Java 1.8
- Spark 2.4.2
- Scala 2.12
- sbt (latest version)

## Configure file
- configure file: src/main/resources/application.conf
-- deploy_master: local[3], yarn, mesos,...
-- bootstrap_server: kafkahost:port,kafkahost-2:port
-- topic_weather: topic push message (producer) after call api and consumer for aggregation 
-- topic_for_stream: any topic (it's for streaming call api)
## My Idea 
- We will parallel call api and push result to kafka topic (It will be stable for any change of aggregation job)
- In aggregation job, we will consume every aggregate_period_sec . Result will be print on screen (local mode) or master node log (if run on cluster)
- I use Dataset for implement business requirement. We can use Dataframe and spark SQL but it use many effort for many windows function. Or use repartition by column and map partition 
## Class
Get data from API: src/main/scala/mps/weather/RestApiToKafka.scala
Aggregation job: src/main/scala/mps/weather/CurrWeatherAgg.scala

## Run locally 

- Install Intellij, Java ,Scala, sbt like version requirement 
- Pull source code
- Open terminal 
- cd /path_to_project
- sbt compile
- Run class RestApiToKafka
- And run: CurrWeatherAgg

## Run on cluster 
- Change deploy master in configure file to: yarn, mesos,...
- Change period, frequency
- Open terminal: 
-- Run command: `sbt package`
-- Run command: `sbt assemblyPackageDependency` 
- we will have 2 file in folder: target/scala-2.12/
-- `mps-weather-demo-assembly-0.1-deps.jar`
-- `scala-2-12_2.12-0.1.0-SNAPSHOT.jar`
- Copy 2 file to cluster and submit job 
Example on YARN:

`spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-cores 1 --executor-memory 1G --class mps.weather.RestApiToKafka --jars mps-weather-demo-assembly-0.1-deps.jar scala-2-12_2.12-0.1.0-SNAPSHOT.jar`

`spark-submit --master yarn --deploy-mode cluster --num-executors 2 --executor-cores 1 --executor-memory 1G --class mps.weather.CurrWeatherAgg --jars mps-weather-demo-assembly-0.1-deps.jar scala-2-12_2.12-0.1.0-SNAPSHOT.jar`

You can see logs on master node of application
