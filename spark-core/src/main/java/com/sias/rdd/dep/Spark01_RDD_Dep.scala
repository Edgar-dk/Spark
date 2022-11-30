package com.sias.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-29 15:57
 * @faction:
 */
object Spark01_RDD_Dep {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparkConf)
    val dataRdd: RDD[String] = sc.textFile("datas/2.txt")
    /*1.toDebugString查看执行的过程也就是数据从那个位置来的*/
    println(dataRdd.toDebugString)
    println("++++++++++++++++++++++++++++++++++")
    val flapMapRdd: RDD[String] = dataRdd.flatMap(_.split(" "))
    println(flapMapRdd.toDebugString)
    println("++++++++++++++++++++++++++++++++++")
    val mapRdd: RDD[(String, Int)] = flapMapRdd.map(word => (word, 1))
    println(mapRdd.toDebugString)
    println("++++++++++++++++++++++++++++++++++")
    val res: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    println(res.toDebugString)
    println("++++++++++++++++++++++++++++++++++")
    res.collect().foreach(println)
    sc.stop()
  }
}
