package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_Max {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    /*1.max得到的最大值，结果只有一个，这个方法，返回的是一个迭代器
    *   下面肯定是不行的，需要把得到的结果，变成迭代器数据形式*/
    val mapRdd: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    mapRdd.collect().foreach(println)
    sc.stop()
  }
}
