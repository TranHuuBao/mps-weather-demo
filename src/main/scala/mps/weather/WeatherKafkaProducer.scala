package mps.weather
import java.util.concurrent.Future
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
class WeatherKafkaProducer[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {

  lazy val producer = createProducer()
  def send(topic: String, key: K, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))

}

object WeatherKafkaProducer {

  import scala.collection.JavaConversions._
  def apply[K, V](config: Map[String, Object]): WeatherKafkaProducer[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new WeatherKafkaProducer(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): WeatherKafkaProducer[K, V] = apply(config.toMap)

}
