package com.Stream

import com.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
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

object SparkStream_KafkaMockData1 {
  def main(args: Array[String]): Unit = {
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))

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

    val clickData: DStream[AdClickData] = kafkaDataDs.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )


    val ds: DStream[((String, String, String), Int)] = clickData.transform(
      rdd => {
        val list: ListBuffer[String] = ListBuffer[String]()

        val conn: Connection = JDBCUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list ")

        val set: ResultSet = pstat.executeQuery()

        while (set.next()) {
          list.append(set.getString(1))
        }
        set.close()
        pstat.close()
        conn.close()

        val filterRdd: RDD[AdClickData] = rdd.filter(
          data => {
            !list.contains(data.user)
          }
        )

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


    ds.foreachRDD(
      rdd => {

        rdd.foreach {
          case ((day, user, ad), count) => {
            println(s"${day} ${user} ${ad} ${count}")
            if (count >= 30) {
              //TODO 一段时间内点击（一个DStream阶段内），统计数量超过点击阈值（30）将用户拉入到黑名单中
              val conn: Connection = JDBCUtil.getConnection
              var sql1 =
                """
                  |insert into black_list (userid) values (?)
                  |on DUPLICATE KEY
                  |UPDATE userid = ?
                """.stripMargin
              JDBCUtil.executeUpdate(conn, sql1, Array(user, user))

              conn.close()
            } else {
              //TODO 没有超过阈值的话，统计一天的点击数量
              /*这次的点击可能是一天的最后一次，也可能是其他时间段，点击没有超过阈值需要到
              * 数据库中，查询一下，这个数据是否有，有的话，进行下一步数量的统计*/
              val conn: Connection = JDBCUtil.getConnection
              var sql2 =
                """
                  |select
                  |    *
                  |from user_ad_count
                  |where dt = ? and userid = ? and adid = ?
                """.stripMargin
              val flg: Boolean = JDBCUtil.isExist(conn, sql2, Array(day, user, ad))

              /*6_04，一开始的时候，指针在第一行之前，第一次调用next的时候，将第一行的数据
              *      取出来*/
              if (flg) {
                //TODO 如果存在数据，更新数据
                var sql3 =
                  """
                    |update user_ad_count
                    |set count = count + ?
                    |where dt = ? and userid = ? and adid = ?
                  """.stripMargin
                JDBCUtil.executeUpdate(conn, sql3, Array(conn, day, user, ad))

                //TODO 数据判断是否超过阈值，超过的话，放在黑名单表里面

                var sql4 =
                  """
                    |select
                    |    *
                    |from user_ad_count
                    |where dt = ? and userid = ? and adid = ? and count >= 30
                  """.stripMargin
                val flg1: Boolean = JDBCUtil.isExist(conn, sql4, Array(day, user, ad))

                if (flg1) {
                  var sql5 =
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid = ?
                    """.stripMargin
                  JDBCUtil.executeUpdate(conn, sql5, Array(user, user))
                }
              } else {
                //TODO 不存在数据的话，插入数据
                var sql6 =
                  """
                    | insert into  user_ad_count ( dt, userid, adid, count) values (?, ?, ?, ?)
                  """.stripMargin
                JDBCUtil.executeUpdate(conn,sql6,Array(day,user,ad,count))
              }
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


