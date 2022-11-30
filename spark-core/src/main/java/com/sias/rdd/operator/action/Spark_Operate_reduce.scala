package com.sias.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-28 21:06
 * @faction:
 */
object Spark_Operate_reduce {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    /*1.reduce不用调用，collect了，collect会调用转换算子
    *   然后提交任务执行，这个行动算子，直接在底层执行*/
    val i: Int = rdd.reduce(_ + _)
    println(i)

    /* //所谓的行动算子，其实就是触发作业（Job）执行的方法
    //底层代码调用的是环境对象的runJob方法
    //底层代码会创建ActiveJob，并提交执行
    rdd.collect()*/


    /*2.collect方法，将不同分区数据，按照分区顺序采集到Driver端内存中，形成数组
    *   */
    val collectRDD: Array[Int] = rdd.collect()
    println(collectRDD.mkString(","))

    //takeOrdered:数据排序后，取前N个数据
    //rdd1.takeOrdered(3)把数据排序，然后取前三个
    val rdd1: RDD[Int] = sc.makeRDD(List(4, 2, 3, 1))
    val takeOrdered: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse) //降序
    println(takeOrdered.mkString(","))
    sc.stop()
  }
}
