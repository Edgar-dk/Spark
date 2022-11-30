package com.sias.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_cogroup {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val joinRdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3)))
    val joinRdd2: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 8),("c", 7)))
    /*1.先进行分组，然后在进行连接，把两个相同的key连接在一起
    *   找个所谓的分组，就是把一个RDD里面的相同的key放在一个组里面，然后在和
    *   另外一个RDD连接起来*/
    val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = joinRdd1.cogroup(joinRdd2)
    cgRDD.collect().foreach(println)
    sc.stop()
  }
}
