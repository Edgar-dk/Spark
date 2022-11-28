package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_partitionByKey {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",2)))

    /*1.将相同的key放在一个分区中
    *   value放在一个可迭代的集合中，集合中的数据，都是value
    *   不用传递参数，直接按照key去放在不同分区中，默认的方式，就是
    *   按照key
    *
    *   (a,CompactBuffer(1, 2, 3))
        (b,CompactBuffer(2))*/
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    groupByKeyRDD.collect().foreach(println)


    /*2.需要在里面传递参数，需要指定数据分区的类型
        第二个参数，也是可迭代的集合，只是，里面存放的是
        每一个元组，元组是每一个数据
    *   (a,CompactBuffer((a,1), (a,2), (a,3)))
        (b,CompactBuffer((b,2)))*/
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
    groupByRDD.collect().foreach(println)
    sc.stop()
  }
}
