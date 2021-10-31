package mps.configs

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object AppConfig {

  def logger = LoggerFactory.getLogger(this.getClass)
  val appConfig: Config = ConfigFactory.load
  def getBoolean(key: String): Boolean = appConfig.getBoolean(key)

  def getInt(key: String): Int = appConfig.getInt(key)

  def getLong(key: String): Long = appConfig.getLong(key)

  def getDouble(key: String): Double = appConfig.getDouble(key)

  def getString(key: String): String = appConfig.getString(key)

  def getString(key: String, default: String): String = Try {
    appConfig.getString(key)
  } match {
    case Success(value) => value
    case Failure(x) => {
      logger.info(s"Can not find any key $key in configuration file. Using default value: $default")
      default
    }
  }

  def getStringList(key: String): Seq[String] = appConfig.getStringList(key)

  def main(args: Array[String]): Unit = {
    println(AppConfig.getInt("aggregate_period_sec"))
  }
}
