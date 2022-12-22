package com.Stream

import com.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */
/*案例二*/
object SparkStream_Case3 {
  def main(args: Array[String]): Unit = {
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(5))
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sias",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val kafkaDataDs: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferBrokers,
      ConsumerStrategies.Subscribe[String, String](Set("siasNew"), kafkaPara)
    )
    val adClickData: DStream[AdClickData] = kafkaDataDs.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    /*一个整体是这么大的（Rdd的20倍，只有当数据处理完毕后，才往下面执行）*/
    val reduceBS: DStream[(Long, Int)] = adClickData.map(
      data => {
        val ts: Long = data.ts.toLong
        val newTs: Long = ts / 10000 * 10000
        (newTs, 1)
      }
    ).reduceByKeyAndWindow((x: Int, y: Int) => {
      x + y
    }, Seconds(60), Seconds(10))

    //reduceDS.print()
    reduceBS.foreachRDD(
      rdd => {
        val list = ListBuffer[String]()
        /*按照相同的key排序*/
        val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
        datas.foreach{
          case ( time, cnt ) => {
            val timeString = new SimpleDateFormat("mm:ss").format(new java.util.Date(time.toLong))
            /*变成json字符串*/
            list.append(s"""{"xtime":"${timeString}", "yval":"${cnt}"}""")
          }
        }
        // 输出文件
        val out = new PrintWriter(new FileWriter(new File("D:\\User1\\rundata\\document\\major\\UnderASophomore\\FBDCode\\BigData\\Spark\\datas\\adclick\\adclick.json")))
        out.println("["+list.mkString(",")+"]")
        out.flush()
        out.close()
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
