package com.core.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-10-15 19:27
 * @faction:
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    /*1.建立和spark之间的连接*/

    /*01.SparkConf存放Spark里面的基础配置对象
    *    setMaster表示地址环境，setAppName是
    *    建立应用程序的名称*/
    val SparkConf = new SparkConf().setMaster("local").setAppName("WordCont")
    val context = new SparkContext(SparkConf)


    /*2.业务操作*/

    /*01.获取一行单词，*/
    val line: RDD[String] = context.textFile("datas")
    /*02.把一行一行的数据拆分成一个一个的单词
    *    对于这种操作是扁平化操作*/
    val value: RDD[String] = line.flatMap(_.split(" "))
    /*03.分组操作，相同的单词，在一个Iterable里面
    *    这个时候，还没有统计单词的个数*/
    val wordGroup: RDD[(String, Iterable[String])] = value.groupBy(word => word)
    val value1: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    /*04.将数据采集到控制台上面*/
    val tuples: Array[(String, Int)] = value1.collect()
    tuples.foreach(println)

    context.stop()
  }
}
