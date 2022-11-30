package com.sias.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_Req {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    /*1.获取文本中的数据
    *   时间戳，省份，城市，用户，广告*/
    val dataRdd: RDD[String] = sc.textFile("datas/agent.log")
    /*2.获取数据后，截取数据*/
    val splitRdd: RDD[((String, String), Int)] = dataRdd.map(
      line => {
        val dataLine: Array[String] = line.split(" ")
        val first: String = dataLine(1)
        val second: String = dataLine(4)
        ((first, second), 1)
      }
    )
    /*3.转换后的数据聚合
    *   按照key将value聚合*/
    val reduceRdd: RDD[((String, String), Int)] = splitRdd.reduceByKey(_ + _)
    /*4.聚合后的结果转换*/
    val mapRdd: RDD[(String, (String, Int))] = reduceRdd.map(
      t => {
        (t._1._1, (t._1._2, t._2))
      }
    )
    /*5.按照省份分组
    *   也就是按照key分组，和的在上面进行过了
    *   这个可迭代的集合中，有多个元组，一个元组是一个广告
    *   按照城市去分到一个组里面*/
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()
    /*6.分组后的数据，降序排列，取前三名
    *   上面的第二个参数，迭代器里的数据，不可以排序，需要转换成，集合（scala中）
    *   然后在排序，按照第二个参数排序，默认是升序，所以后面Ordering是降序的操作
    *   take是取前三个
    *
    *   注意：这个降序是，一个城市里面，这些广告被点击量的降序，不是城市和城市之间
    *   的是，是城市内部的*/
    val resRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        iter.toList.sortBy(t1 => t1._2)(Ordering.Int.reverse).take(3)
      }
    )
    resRdd.collect().foreach(println)
    sc.stop()
  }
}
