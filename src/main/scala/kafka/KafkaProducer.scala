package kafka

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.io.Source

object KafkaProducer {

  def main(args: Array[String]): Unit = {

    println("Kafka Producer Application Started....")

    val orderItemsMessages = Source.fromFile("/home/knoldus/Downloads/retail_db/order_items/part-00000").getLines.toList
    val ordersMessages = Source.fromFile("/home/knoldus/Downloads/retail_db/orders/part-00000").getLines.toList

    val topicOrderItems = "order_items"
    val topicOrders = "orders"

    val conf = ConfigFactory.load
    val envProps = conf.getConfig(args(0))

    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      envProps.getString("bootstrap.server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG,
      "ProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")

    val producerOrderItems = new KafkaProducer[String, String](props)

      orderItemsMessages.foreach(orderItem => {
        println(orderItem)
        val record = new ProducerRecord[String, String](topicOrderItems, orderItem)
        producerOrderItems.send(record)
      })

    val producerOrders = new KafkaProducer[String, String](props)

    val producerOrdersFuture = Future {
      ordersMessages.foreach(order => {
        println(order)
        val record = new ProducerRecord[String, String](topicOrders, order)
        producerOrders.send(record)
      })
    }

    producerOrdersFuture.onComplete {
      case Success(value) => producerOrders.close()
      case Failure(e) => println(s"It have failed with : $e")
    }

    producerOrderItems.close()

    Thread.sleep(2000)
  }

}
