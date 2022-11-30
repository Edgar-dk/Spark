package com.sias.rdd.base

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-03 20:36
 * @faction:
 */


/*1.指定数据来自那个文件*/

object File {
  def main(args: Array[String]): Unit = {
    /*1.准备环境
    *   第一个，local后面不写*的话，表示单个核数，写的话，是自己电脑上
    *   最大核数，去运行*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val context = new SparkContext(sparkConf)

    /*2.RDD的创建
    *   内存在创建RDD，将集合数据，在内存中处理
    *   下面是直接监控这个文件夹下面的所有文件
    *   还有监控指定的文件，这种方式是相对路径监控的方式
    *   文件可以是普通的文件，或者是分布式文件系统中的文件*/
    val rdd: RDD[String] = context.textFile("datas")
    rdd.collect().foreach(println)


    /*3.关闭环境*/
    context.stop()
  }
}
