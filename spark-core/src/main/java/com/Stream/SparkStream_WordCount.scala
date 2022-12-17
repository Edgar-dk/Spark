package com.Stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.SearchedCaseContext
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */
object SparkStream_WordCount {
  def main(args: Array[String]): Unit = {

    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))

    /*2.获取端口数据
    *   localhost是本地地址，后面是这个IP地址下面的端口*/
    val line: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val word: DStream[String] = line.flatMap(_.split(" "))

    val wordToOne: DStream[(String, Int)] = word.map((_, 1))

    val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
    wordToCount.print()

    /*3.采集器需要一直开启，一直监视着端口，开启的话，就
    *   可以收到消息*/
    ssc.start()

    /*3_01，main方法，也需要一直开启着，main线程的等待，main关闭的话，也接收不到消息了*/
    ssc.awaitTermination()
  }
}
