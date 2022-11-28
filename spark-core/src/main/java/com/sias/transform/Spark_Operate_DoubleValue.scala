package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_DoubleValue {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6))
    val rdd7: RDD[String] = sc.makeRDD(List("3", "4", "5", "6"))


    //交集、并集和差集要求两个数据源数据类型要保持一致
    //拉链操作两个数据源数据类型可以不一致

    //交集
    val inter: RDD[Int] = rdd1.intersection(rdd2)
    println(inter.collect().mkString(","))
    //并集:1,2,3,4,3,4,5,6
    val rdd4: RDD[Int] = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))

    //差集:1,2
    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))

    //拉链:(1,3),(2,4),(3,5),(4,6)
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))

    //(1,3),(2,4),(3,5),(4,6)
    val rdd8: RDD[(Int, String)] = rdd1.zip(rdd7)
    println(rdd8.collect().mkString(","))

    sc.stop()
  }
}
