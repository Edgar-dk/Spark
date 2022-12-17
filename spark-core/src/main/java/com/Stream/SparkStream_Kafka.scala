package com.Stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */

object SparkStream_Kafka {
  def main(args: Array[String]): Unit = {
    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))

    //3.定义 Kafka 参数，连接kafka的配置参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sias", "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    /*2.将Kafka的工具包导入进来
    *   泛型地方，第一个参数，是key的类型，
    *   第二个参数是value的类型
    *   Location...将采集和计算的数据，用框架的逻辑处理，将这些数据放在那些节点上
    *   Consumer...消费者端策略，订阅的是sias，kafkaPara是连接的地址，以及消费信息*/
    val kafkaDataDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String, String](Set("sias"), kafkaPara)
    )
    /*3.得到数据之后干什么，需要处理数据，只要第二个数据，将第二个数据打印出来*/
    kafkaDataDs.map(_.value()).print()
    ssc.start()
    ssc.awaitTermination()
  }


}
