package com.Stream
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Properties, Random}
import scala.collection.mutable.ListBuffer
/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */
object SparkStream_MockData {
  def main(args: Array[String]): Unit = {
    // 创建配置对象
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // 根据配置创建 Kafka 生产者，将配置参数，填写到kafka生产端
    val producer = new KafkaProducer[String, String](prop)
    while (true) {
      mockdata().foreach(
        data => {
          /*topic是主题，data是传递的数据是什么*/
          val record = new ProducerRecord[String, String]("siasNew", data)
          producer.send(record)
          println(data)
        }
      )
      /*发送一条数据，等待一段时间*/
      Thread.sleep(2000)
    }
  }
  def mockdata() = {
    val list: ListBuffer[String] = ListBuffer[String]()
    val areaList: ListBuffer[String] = ListBuffer[String]("华北", "华东", "华南")
    val cityList: ListBuffer[String] = ListBuffer[String]("北京", "上海", "深圳")
    /*int x=new Random.nextInt(100);
　　　　则x为一个0~99的任意整数
       ListBuffer是一个可变的List，可以往这个List添加数据*/
    for (i <- 1 to new Random().nextInt(50)) {
      val area: String = areaList(new Random().nextInt(3))
      val city: String = cityList(new Random().nextInt(3))
      val userId: Int = new Random().nextInt(6)
      val adid: Int = new Random().nextInt(6)
      list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userId} ${adid}")
    }
    list
  }
}
