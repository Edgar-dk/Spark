package com.Stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */

object SparkStream_state_window {
  def main(args: Array[String]): Unit = {
    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))
    val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = data.map((_, 1))

    /*1.滑动窗口是，一个DStream的整数倍，因为在收集数据的时候，不能是一半的数据
    *   下面是将2个DStream合成一个整体，整体数据计算，窗口滑动到第一个阶段的时候
    *   将第一个阶段想要统计的数据，统计好，放在缓存中，到第二个阶段，在统计想要
    *   的数据，然后两个阶段的数据合并
    *
    *   注意：这种容易出现问题，走到第三个阶段，本质上是第三个和第四个数据合并的（按照下面2个阶段），
    *   这个时候是，第三个和第二个合并了，容易造成重复数据，可以改变滑动步长，一下子
    *   滑动这么多的，阶段,第二个参数，是滑动的步长，一下子滑动到下个整体阶段*/
    val windowDs: DStream[(String, Int)] = wordToOne.window(Seconds(6),Seconds(6))
    val wordCount: DStream[(String, Int)] = windowDs.reduceByKey(_ + _)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
