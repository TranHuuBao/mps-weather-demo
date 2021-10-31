package mps.weather

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.common.serialization.{ByteArraySerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, RecordMetadata}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaj.http.{Http, HttpResponse}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils
import mps.configs.AppConfig

object RestApiToKafka {
  val appKey: String = AppConfig.getString("app_key")
  val deployMode: String = AppConfig.getString("deploy_master")
  val BOOTSTRAP_SERVER: String = AppConfig.getString("bootstrap_server")
  val topic: String = AppConfig.getString("topic_weather")
  val city: Array[String] = AppConfig.getString("locations").split(",")
  val topicStream: String = AppConfig.getString("topic_for_stream")
  val duration: Int = AppConfig.getInt("frequency_sec")
  def getDataWeatherAPI: String => String = (cityName: String) => {
    val endpoint = "http://api.openweathermap.org/data/2.5/weather"
    val response: HttpResponse[String] = Http(endpoint)
      .params(("q", cityName), ("appid", appKey), ("units", "metric")).asString
    response.body
  }

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVER,
      "group.id" -> "demo",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val ssc = {
      val sparkConf = new SparkConf().setAppName("Publish weather data to kafka").setMaster(deployMode)
      new StreamingContext(sparkConf, Seconds(duration))
    }
    val spark = SparkSession.builder()
      //      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val udfCallApiWeather = udf(getDataWeatherAPI, StringType)

    val cityDF = spark.sparkContext.parallelize(city).toDF("city").cache()
    val dsStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(topicStream), kafkaParams)
    )
    val kafkaProducer: Broadcast[WeatherKafkaProducer[Array[Byte], String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", BOOTSTRAP_SERVER)
        p.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      spark.sparkContext.broadcast(WeatherKafkaProducer[Array[Byte], String](kafkaProducerConfig))
    }

    dsStream.foreachRDD(rdd => {
      val responseDF = cityDF.withColumn("response", udfCallApiWeather(col("city")))
      responseDF.rdd.foreachPartition { iter =>
        val metadata: Stream[Future[RecordMetadata]] = iter.map(record =>
          kafkaProducer.value.send(topic, record.getAs[String]("city").getBytes(),record.getAs[String]("response"))
        ).toStream
        metadata.foreach { metadata => metadata.get() }
      }
    })
    ssc.start()
    ssc.awaitTermination()


  }
}
