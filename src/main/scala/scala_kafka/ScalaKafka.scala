package scala_kafka

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.spark._
import org.apache.spark.streaming._

object ScalaKafka {
  def main(args: Array[String]): Unit = {
    ////    val conf = new SparkConf()
    //    val conf = new SparkConf().setAppName("AppName").setMaster("local[1]")
    //    val ssc = new StreamingContext(conf,Seconds(1))
    import org.apache.log4j.BasicConfigurator
    BasicConfigurator.configure()


    val props: Properties = new Properties()
    props.put("group.id", "test")
    //    props.put("bootstrap.servers","127.0.0.1:9092")
    props.put("bootstrap.servers", "10.82.100.33:9092,10.82.100.34:9092,10.82.100.35:9092")

    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")

    println(0)
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("disbursement"))

    // if from beginning
    consumer.seekToBeginning(consumer.assignment())
    println(1)
    while (true) {
      val records = consumer.poll(100)
      println(2)
      import scala.collection.JavaConversions._
      for (record <- records) {
        //        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset, record.key, record.value)
        println(record.offset(), record.value())
      }
      println("end loop")
    }

  }
}
