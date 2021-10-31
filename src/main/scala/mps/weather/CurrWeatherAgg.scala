package mps.weather

import java.sql.Timestamp

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.google.gson.{Gson, JsonObject}
import mps.configs.AppConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window




object CurrWeatherAgg {
  val appKey: String = AppConfig.getString("app_key")
  val deployMode: String = AppConfig.getString("deploy_master")
  val BOOTSTRAP_SERVER: String = AppConfig.getString("bootstrap_server")
  val topic: String = AppConfig.getString("topic_weather")
  val duration: Int = AppConfig.getInt("aggregate_period_sec")

  case class Weather(city: String, datetime: Long, responseCode: Int,
                     temperature: Option[Long], description: Option[String], timeDt: Option[Long])
  case class WeatherAgg(city: String, datetime: Long, responseCode: Int,
                     temperature: Option[Long], description: Option[String], timeDt: Option[Long], periodId: Long)
  implicit def ordered: Ordering[Timestamp] = new Ordering[Timestamp] {
    def compare(x: Timestamp, y: Timestamp): Int = x compareTo y
  }
  def exactWeather= (rdd: RDD[ConsumerRecord[String, String]]) => {
    rdd.map(record => {
      val city = record.key()
      val datetime = record.timestamp()
      val jsonObj = new Gson().fromJson(record.value(), classOf[JsonObject])
      val responseCode = jsonObj.get("cod").getAsInt
      if (responseCode != 200)
        Weather(city, datetime, responseCode, None, None, None)
      else {
        val temperature = jsonObj.getAsJsonObject("main").get("temp").getAsLong
        val timeDt = jsonObj.get("dt").getAsLong
        val description = jsonObj.getAsJsonArray("weather").get(0).getAsJsonObject.get("main").getAsString
        Weather(city, datetime, responseCode, Some(temperature), Some(description), Some(timeDt))
      }

    })
  }
  def main(args: Array[String]): Unit = {
    val ssc = {
      val sparkConf = new SparkConf().setAppName("Weather").setMaster(deployMode)
      new StreamingContext(sparkConf, Seconds(duration))
    }
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> BOOTSTRAP_SERVER,
      "group.id" -> "consumer",
      "auto.offset.reset" -> "latest",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val dsStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(topic), kafkaParams))
      .transform(exactWeather)
    import spark.implicits._
    dsStream.foreachRDD(rdd => {
        val overCityMinutes = Window.partitionBy("city", "minutes")
        val overCityMinutesDesc = Window.partitionBy("city", "minutes", "description")
        val result = rdd.toDS()
          .withColumn("periodId",  (col("datetime")/(1000*duration)).cast(LongType))
          .withColumn("datetime", to_utc_timestamp(to_timestamp(col("datetime")/1000), "Asia/Ho_Chi_Minh"))
          .as[WeatherAgg]
          .groupByKey(a => (a.periodId, a.city, a.responseCode))
          .mapGroups{
            case ((periodId, city, responseCode), events)=>{
              val evtArr = events.toArray.sortBy(_.datetime)
              val firstEvt = evtArr.head
              val lastEvt = evtArr.last
              if (responseCode==200) {
                  val avg = evtArr.map(_.temperature.get).sum / evtArr.length
                  val aggDesc = evtArr.map(x => x.description.get -> (1, x.datetime))
                    .groupBy(_._1).mapValues(seq => (seq.map(_._2._1).sum, seq.map(_._2._2).min))
                    .toList.sortBy(_._2).reverse
                  val maxCntDesc = aggDesc.head._2._1
                  val desc = aggDesc.filter(_._2._1 == maxCntDesc).minBy(_._2._2)._1
                  val diff = firstEvt.temperature.get - lastEvt.temperature.get
                (city, periodId, responseCode, Some(avg), Some(diff), Some(desc))
              }
              else
                (city, periodId, responseCode, None, None, None)
            }
          }.toDF("location", "periodId", "responseCode", "avg", "diff", "desc")
        .withColumn("datetime", date_format(to_utc_timestamp(to_timestamp(col("periodId")*duration),
          "Asia/Ho_Chi_Minh"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("description", when(col("responseCode")===lit(200), concat_ws(", "
          ,concat_ws(": ", lit("location"), col("location"))
          ,concat_ws(": ", lit("avg_temperature"), col("avg"))
          ,concat_ws(": ", lit("temperature_diff"), col("diff"))
          ,concat_ws(": ", lit("description"), col("desc"))
        )).otherwise(concat_ws(": ", lit("location"), col("location"), lit("is invalid"))))
        .groupBy("datetime").agg(concat_ws("; ", collect_list(col("description"))).as("description"))
        .withColumn("console", concat_ws(":", col("datetime"), col("description") ))
        .select("console")
        result.collect().foreach(println)

      })
    ssc.start()
    ssc.awaitTermination()
  }
}
