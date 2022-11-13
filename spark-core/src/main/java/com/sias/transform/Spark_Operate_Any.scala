package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_Any {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    /*1.在上面一个步骤中，数据已经，进入到分区中了，分区中的数据，都是单个的
    *   下面，把分区中，单个的数据整合在一起，变成一个数组，然后在把一个分区中的数据
    *   遍历出去，分区分区的数据，在一个地方，一并输出*/
    val glomArry: RDD[Array[Int]] = rdd.glom()
    glomArry.collect().foreach(data => {
      println(data.mkString(","))
    })

    /*2.plom的方式，求解，单个分区的最大值，以及分区之间最大值求和问题
    *   在map中统计最大的数，然后在用得到的数，求解，总和*/
    val glomArry1: RDD[Array[Int]] = rdd.glom()
    val Max: RDD[Int] = glomArry1.map(
      data => {
        data.max
      }
    )
    println(Max.collect().sum)

    /*3.数据分组存放
    *   不指定分组的方式，会按照，默认的方式，也就是
    *   这组数据中，key的值（数据的值），相同的数据，放在一个组中
    *   也可以指定好规则分区，下面是一种分组的规则*/
    def groupFunction(num:Int):Int={
      num % 2
    }

    val groupValue: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    groupValue.collect().foreach(println)

    /*4.按照首先字母分组*/
    val rdd1: RDD[String] = sc.makeRDD(List("Hello","Spark", "HeP","SL"), 2)
    val groupCharAt: RDD[(Char, Iterable[String])] = rdd1.groupBy(_.charAt(0))
    groupCharAt.collect().foreach(println)
    sc.stop()
  }
}
