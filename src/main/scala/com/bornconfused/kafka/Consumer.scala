package com.bornconfused.kafka

/**
  * Created by ranand on 6/23/2017 AD.
  */
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

object Consumer extends App {

  import java.util.Properties

  val TOPIC = "test"

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")

  //disable auto commit
  props.put("enable.auto.commit", "false")
  // props.put("auto.commit.interval.ms", "1000")
  // props.put("session.timeout.ms", "30000")
  //consumer group
  props.put("group.id", "ConsumerGroup1")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while (true) {
    val records = consumer.poll(100)
    for (record <- records.asScala) {
      println(record)
    }
    //consumer.commitSync()
  }
}
