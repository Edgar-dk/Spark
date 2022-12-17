package com.core.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-03 20:36
 * @faction:
 */


/*1.在内存中创建一个RDD
*   就是在内存中创建一个逻辑方法*/

object Memory {
  def main(args: Array[String]): Unit = {
    /*1.准备环境
    *   第一个，local后面不写*的话，表示单个核数，写的话，是自己电脑上
    *   最大核数，去运行*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    /*2.RDD的创建
    *   内存在创建RDD，将集合数据，在内存中处理*/
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    /*00.并行的含义，多少任务，需要根据任务的数量，以及核数的多少去判断怎么去执行
    *    一个任务对应着一个核数的话，有2个任务，2个核数，这个时候，是并行的含义
    *    ，但是在这个地方，执行的含义不明确，*/
    //    val rdd: RDD[Int] = context.parallelize(seq)

    /*上面一种创建方式，从语境上不太好理解，可以用，makeRdd,在内存中创建
    * 其实底层还是parallelize*/
    val rdd: RDD[Int] = context.makeRDD(seq)
    rdd.saveAsTextFile("output")

    /*01.只有触发了collect程序才会执行程序*/
    rdd.collect().foreach(println)

    /*2.关闭环境*/
    context.stop()
  }
}
