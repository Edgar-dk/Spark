package com.Stream

import com.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.parquet.schema.Types.ListBuilder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */

object SparkStream_KafkaMockData {
  def main(args: Array[String]): Unit = {
    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))

    //3.定义 Kafka 参数，连接kafka的配置参数
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "sias",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
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
      ConsumerStrategies.Subscribe[String, String](Set("siasNew"), kafkaPara)
    )
    /*3.得到数据之后干什么，将得到的数据，放在样例类里面*/
    val clickData: DStream[AdClickData] = kafkaDataDs.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    /*通过JDBC周期性的获取黑名单数据*/
    val ds: DStream[((String, String, String), Int)] = clickData.transform(
      rdd => {
        val list: ListBuffer[String] = ListBuffer[String]()
        /*1.获取连接对象*/
        val conn: Connection = JDBCUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list ")

        /*2.使用完毕之后，将这些连接关闭，防止，资源耗尽
        *   连接一直占用着一定限制的资源，不关闭的话，一直
        *   消耗这些资源，可以理解成一个线程，在来多个查询
        *   的话，很多的线程都去在占用这些资源，一直消耗着的话
        *   再来的就不能连接上这个数据库了*/

        /*3.executeQuery是一个查询操作*/
        val set: ResultSet = pstat.executeQuery()

        while (set.next()) {
          list.append(set.getString(1))
        }
        set.close()
        pstat.close()
        conn.close()
        /*4.判断点击的用户是否在黑名单里面
        *   本质上，data是rdd数据，rdd又是clickData，
        *   这个clickData又是AdClickData这个样例类对象
        *   看看点击的数据，是否在这个黑名单里面，在的话不保留，
        *   不在的话保留，所谓的保留和不保留就是是否往下面执行，保留的话，往下面执行
        *   不保留的话，将这条数据筛选出去，不往下执行了*/
        val filterRdd: RDD[AdClickData] = rdd.filter(
          data => {
            !list.contains(data.user)
          }
        )

        /*5.如果用户不在黑名单里面，进行统计数量（每个采集周期的方式进行）
        *   上面的程序下来后，变形这些，数据，将数据格式发生变化，format是格式化，
        *   将ts格式化成day的方式，由，时间戳格式化成，day方式，然后这么多的数据
        *   都经过这个map，最终数据按照相同key的方式对value统计合并*/
        filterRdd.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day: String = sdf.format(new Date(data.ts.toLong))
            val user: String = data.user
            val ad: String = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    /*6.使用foreachRdd，目的是对里面的每一个Rdd操作，将操作的结果输出到外部框架，例如数据库，文件等*/
    ds.foreachRDD(
      rdd => {

        /*6_01，具体到Rdd的操作*/
        rdd.foreach {
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")
            if (count >= 30) {
              //TODO 一段时间内点击（一个DStream阶段内），统计数量超过点击阈值（30）将用户拉入到黑名单中
              val conn: Connection = JDBCUtil.getConnection
              /*6_02，对于操作的数据而言，不查询数据库中是否有数据直接插入数据
              *       下一次，在来插入的数据的话，也是按照这个userid插入的，有这个
              *       id的话，更新这个id对应的数据*/
              val pstat: PreparedStatement = conn.prepareStatement(
                """
                  |insert into black_list (userid) values (?)
                  |on DUPLICATE KEY
                  |UPDATE userid = ?
                """.stripMargin)
              /*6_03，参数1，是操作第一个？，第二个参数，对第一个？位置上设置的值*/
              pstat.setString(1, user)
              pstat.setString(2, user)
              pstat.executeUpdate()
              pstat.close()
              conn.close()
            } else {
              //TODO 没有超过阈值的话，统计一天的点击数量
              /*这次的点击可能是一天的最后一次，也可能是其他时间段，点击没有超过阈值需要到
              * 数据库中，查询一下，这个数据是否有，有的话，进行下一步数量的统计*/
              val conn: Connection = JDBCUtil.getConnection
              val pstat: PreparedStatement = conn.prepareStatement(
                """
                  |select
                  |    *
                  |from user_ad_count
                  |where dt = ? and userid = ? and adid = ?
                """.stripMargin)
              pstat.setString(1, day)
              pstat.setString(2, user)
              pstat.setString(3, ad)


              val set: ResultSet = pstat.executeQuery()
              /*6_04，一开始的时候，指针在第一行之前，第一次调用next的时候，将第一行的数据
              *      取出来*/
              if (set.next()) {
                //TODO 如果存在数据，更新数据
                val statement: PreparedStatement = conn.prepareStatement(
                  """
                    |update user_ad_count
                    |set count = count + ?
                    |where dt = ? and userid = ? and adid = ?
                  """.stripMargin)
                statement.setInt(1, count)
                statement.setString(2, day)
                statement.setString(3, user)
                statement.setString(4, ad)
                statement.executeUpdate()
                statement.close()

                //TODO 数据判断是否超过阈值，超过的话，放在黑名单表里面
                val statement1: PreparedStatement = conn.prepareStatement(
                  """
                    |select
                    |    *
                    |from user_ad_count
                    |where dt = ? and userid = ? and adid = ? and count >= 30
                  """.stripMargin)
                statement1.setString(1, day)
                statement1.setString(2, user)
                statement1.setString(3, ad)
                val set1: ResultSet = statement1.executeQuery()
                if (set1.next()) {
                  val pstat3 = conn.prepareStatement(
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid = ?
                    """.stripMargin)
                  pstat3.setString(1, user)
                  pstat3.setString(2, user)
                  pstat3.executeUpdate()
                  pstat3.close()
                }
                set1.close()
                statement1.close()
              } else {
                //TODO 不存在数据的话，插入数据
                val pstat1: PreparedStatement = conn.prepareStatement(
                  """
                    | insert into  user_ad_count ( dt, userid, adid, count) values (?, ?, ?, ?)
                  """.stripMargin)
                pstat1.setString(1, day)
                pstat1.setString(2, user)
                pstat1.setString(3, ad)
                pstat1.setInt(4, count)

                pstat1.executeUpdate()
                pstat1.close()
              }
              set.close()
              pstat.close()
              conn.close()
            }
          }
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}


