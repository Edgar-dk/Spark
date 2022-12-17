package com.Stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */

object SparkStream_state {
  def main(args: Array[String]): Unit = {
    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))

    /*1_01，设置检查点，将缓存过来的数据永久保存在磁盘中
    *       目的：下面第一份数据放在缓存区中，缓存区在内存中保存
    *            内存的形式，易丢失数据，所以设置在磁盘中保存*/
    ssc.checkpoint("cp")

    // 无状态数据操作，只对当前的采集周期内的数据进行处理
    // 在某些场合下，需要保留数据统计结果（状态），实现数据的汇总
    // 使用有状态操作时，需要设定检查点路径
    val datas = ssc.socketTextStream("localhost", 9999)
    val wordToOne = datas.map((_, 1))
    /*2.根据相同的key对数据状态更新
        将多个时间段内的数据合并，多个DStream，合并成一个，一个时间段是一个DStream
        新来的数据和缓存区中的数据按照规则合并（相同key的规则下合并）
    *   第一个参数：相同key的value数据
    *   第二个参数：缓存区相同key的value数据*/
    val state: DStream[(String, Int)] = wordToOne.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount: Int = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    state.print()
    ssc.start()
    ssc.awaitTermination()
  }


}
