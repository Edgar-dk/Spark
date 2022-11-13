package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_Split {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[String] = sc.makeRDD(List("hello word", "hello spark"))
    /*1.直接把list，集合里面的数据一个一个的拆分开
    *   然后在把整体，按照集合的方式输出*/
    val word: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )
    word.collect().foreach(println)

    /*2.模式匹配
    *   处理不同类型的数据，当数据类型是list，原封不动的处理，直接把这个list拆开就可以
    *   当不是list，在外面加上一个list，变成一个集合，最后在集合整体的形式输出*/
    val rdd2: RDD[Any] = sc.makeRDD(List(List(1, 2), 3, List(4, 5)))
    val type1: RDD[Any] = rdd2.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )
    type1.collect().foreach(println)
    sc.stop()
  }
}
