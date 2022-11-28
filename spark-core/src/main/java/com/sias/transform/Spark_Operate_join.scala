package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_join {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    /*1.join是把相同的key连接在一个，没有相同的key的话，不会连接，存在多个的话，不同之间的rdd会相互
    *   的连接，对应，*/
    val joinRdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3)))
    val joinRdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 8)))
    val joinOut: RDD[(String, (Int, Int))] = joinRdd1.join(joinRdd2)
    joinOut.collect().foreach(println)
    sc.stop()
  }
}
