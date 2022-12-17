package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_union {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local").setAppName("Union")
    val sc = new SparkContext(sparkCof)

    val dataRDD1 = sc.makeRDD(List(1,2,3,4))
    val dataRDD2 = sc.makeRDD(List(3,4,5,6))
    val dataRDD = dataRDD1.union(dataRDD2)
    println(dataRDD)
    println(dataRDD.foreach(println))
    dataRDD.collect().foreach(println)
    sc.stop()
  }
}
