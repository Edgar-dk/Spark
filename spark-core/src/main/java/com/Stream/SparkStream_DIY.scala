package com.Stream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Random
import scala.collection.mutable

/**
 * @author Edgar
 * @create 2022-12-12 21:27
 * @faction:
 */

//将RDD放在，队列中，从队列中，一个一个的取出来
object SparkStream_DIY {
  def main(args: Array[String]): Unit = {
    /*1.准备好环境，关于第二个参数中，Second，3表示，3秒处理*/
    val sparConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparConf, Seconds(3))
    /*2.执行自定义的采集器，将这个类注册到这个对象中，默认执行start方法*/
    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    messageDS.print()
    ssc.start()
    ssc.awaitTermination()
  }
  /*1.自定义数据采集器
  *   第一个参数：数据的类型，第二个，传递的参数（设置的是放在内存当中）
  *   重写方法上，第一个启动采集器的时候，执行onStart，关闭的时候，执行onStop
  *   使用Random随机的采集数据，*/
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true;
    override def onStart(): Unit = {
      new Thread(new Runnable {
        /*1_01，数据执行的方法*/
        override def run(): Unit = {
          while (flag){
            val message: String = "采集的数据为："+ new Random().nextInt(10).toString
            /*将采集过来的数据，封装在内存中，也就是上面StorageLevel设置的保存模式，采集一个数据后，睡眠500毫秒*/
            store(message)
            Thread.sleep(500)
          }
        }
      }).start()
    }
    /*当执行stop方法，标记是false，停止while的执行*/
    override def onStop(): Unit = {
      flag=false;
    }
  }
}
