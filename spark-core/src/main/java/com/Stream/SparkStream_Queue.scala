package com.Stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */

//将RDD放在，队列中，从队列中，一个一个的取出来
object SparkStream_Queue {
  def main(args: Array[String]): Unit = {

    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))


    //3.创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //4.创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
    //5.处理队列中的 RDD 数据
    val mappedStream = inputStream.map((_,1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    /*3.采集器需要一直开启，一直监视着端口，开启的话，就
    *   可以收到消息*/
    ssc.start()



    //8.循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    /*3_01，main方法，也需要一直开启着，main线程的等待，main关闭的话，也接收不到消息了*/
    ssc.awaitTermination()
  }
}
