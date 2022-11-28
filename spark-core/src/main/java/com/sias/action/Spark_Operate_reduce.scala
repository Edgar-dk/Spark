package com.sias.action

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
    sc.stop()
  }
}
