package com.sias.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_sortBy {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)

    /*1.数据排序操作
    *   排完序后，然后在把数据按照分区的规则，分布到个个分区里面
    *   第一个num是以前乱序的数据，第二个，num是排好序列的数据*/
    val rdd: RDD[Int] = sc.makeRDD(List(6,2,4,5,1,3),2)
    val sortBy: RDD[Int] = rdd.sortBy(num => num)
    sortBy.saveAsTextFile("output")
    sc.stop()
  }
}
