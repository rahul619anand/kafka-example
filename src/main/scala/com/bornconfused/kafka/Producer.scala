package com.bornconfused.kafka

/**
  * Created by ranand on 6/23/2017 AD.
  */
object Producer extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val TOPIC="test"

  for(i<- 1 to 100000){
    val record = new ProducerRecord(TOPIC, "key", s"hello $i")
    val recordMetadata = producer.send(record).get()
    println("============ metadata =================" + recordMetadata.offset())
  }

  val record = new ProducerRecord(TOPIC, "key", "the end "+new java.util.Date)
  producer.send(record)

  producer.close()
}