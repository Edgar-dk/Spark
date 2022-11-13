package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


import java.text.SimpleDateFormat
import java.util.Date
/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_AcountTime {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    //从服务器日志数据 apache.log 中获取每个时间段访问量。
    // TODO 算子 —— groupBy
    val rdd = sc.textFile("datas/apache.log")
    /*1.在没有分组之前，得到的都是一个时间，一个1
    *   分组之后，第一个是时间，后面是，没有分组之前得到的数据，
    *   只是这些数据，呈现迭代器的形式，相同的数据，都存在第一个key的后面
    *   对于groupBy而言，第一个_是一个元组，第二个，_1是从元组中获取第一个数据*/
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      f = line => {
        val fields: Array[String] = line.split(" ")
        val date: String = fields(3)
        /*01.创建日期格式
             将这个字符串转换成日期格式的数据*/
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date1: Date = sdf.parse(date)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date1)
        (hour, 1)
      }
    ).groupBy(_._1)
    /*2.将上面的数据，做一个统计*/
    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
