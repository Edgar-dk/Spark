package com.core.rdd.persist

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * @author Edgar
 * @create 2022-11-29 19:31
 * @faction:
 */
object Spark_Operate_persist {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.makeRDD(List("Hello Spark", "Hello Scala"))
    val flapMapRdd: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRdd: RDD[(String, Int)] = flapMapRdd.map(word => {
      println("+++++++++++++")
      (word, 1)
    })
    /*1.将mapRdd中的数据，放在缓存中，这个时候，运算，groupByKey的时候，直接获取数据运算
    *   就不用从头开始执行了，cache是把数据，放在内存中，
    *   persist，是放在磁盘中，下面的persist虽然是保存在磁盘中，是临时的文件，执行结束后，会把这个文件删除*/
//    mapRdd.cache()
    mapRdd.persist(StorageLevel.DISK_ONLY)

    val res: RDD[(String, Int)] = mapRdd.reduceByKey(_ + _)
    res.collect().foreach(println)

    val groupRDD: RDD[(String, Iterable[Int])] = mapRdd.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
