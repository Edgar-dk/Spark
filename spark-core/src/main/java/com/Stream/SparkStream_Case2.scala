package com.Stream
import com.Stream.SparkStream_KafkaMockData.AdClickData
import com.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat
import java.util.Date
/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */
/*案例二*/
object SparkStream_Case2 {
  def main(args: Array[String]): Unit = {
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sias", "key.deserializer" ->
        "org.apache.kafka.common.serialization.StringDeserializer",
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
    val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
      data => {
        val sdf1 = new SimpleDateFormat("yyyy-MM-dd")
        val day: String = sdf1.format(new Date(data.ts.toLong))
        val area: String = data.area
        val city: String = data.city
        val ad: String = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)
    reduceDS.foreachRDD(
      rdd => {
        /*在每一个分区里面创建一个JDBC的连接，不用在每一条数据里面创建一个连接*/
        rdd.foreachPartition(
          iter => {
            val conn: Connection = JDBCUtil.getConnection
            val pstat: PreparedStatement = conn.prepareStatement(
              """
                | insert into area_city_ad_count ( dt,area,city,adid,count)
                | values (?,?,?,?,?)
                | on duplicate key
                | update count = count + ?
              """.stripMargin)
            /*下面是，针对的是每一个数据*/
            iter.foreach {
              case ((day, area, city, ad), sum) => {
                pstat.setString(1,day)
                pstat.setString(2,area)
                pstat.setString(3,city)
                pstat.setString(4,ad)
                pstat.setInt(5,sum)
                pstat.setInt(6,sum)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            conn.close()
          }
        )
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
