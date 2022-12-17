package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SPARK_BRANCH, SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-11 20:23
 * @faction:
 */
object Spark_transform_Test {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    /*1.先从文件中按照行，把数据全部读取到rdd中*/
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    /*1.rdd是一行数据，把一行数据传递到map里面处理，map中，line就是得到处理后的一行数据
    *   然后在把数据，rdd中的数据，一行一行的处理，最终把所有数据给rddMap，然后在按照行
    *   一行一行的输出*/
    val rddMap: RDD[String] = rdd.map(
      line => {
        val strings: Array[String] = line.split(" ")
        strings(6)
      }
    )
    rddMap.collect().foreach(println)
    sc.stop()
  }
}
