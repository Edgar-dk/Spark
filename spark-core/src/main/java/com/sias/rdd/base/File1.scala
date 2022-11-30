package com.sias.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-03 20:36
 * @faction:
 */


/*1.文件中创建RDD
*   文件中的数据，作为处理的数据源*/

object File1 {
  def main(args: Array[String]): Unit = {
    /*1.准备环境
    *   第一个，local后面不写*的话，表示单个核数，写的话，是自己电脑上
    *   最大核数，去运行*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    /*2.创建RDD
    *   读取过来的数据，进行标记上，标记数据是来自哪一个文件*/
    val sc: RDD[(String, String)] = context.wholeTextFiles("datas")
    sc.saveAsTextFile("output")

    sc.collect().foreach(println)



    /*3.关闭环境*/
    context.stop()
  }
}
