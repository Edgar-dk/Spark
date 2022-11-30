package com.sias.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-29 19:31
 * @faction:
 */
object Spark_Operate_CheckPoint {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparkConf)
    /*1.设置好要保存的地址*/
    sc.setCheckpointDir("CheckpointDir")
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val flapMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flapMapRdd.map(word => {
      println("+++++++++++++")
      (word, 1)
    })

    /*2.启用检查点，以后用到的，直接从落在硬盘中的数据*/
    mapRdd.checkpoint()

    val res: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    res.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
