package com.sias.rdd.part

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-29 19:31
 * @faction:
 */

/*一，自定义分区*/
object Spark_Operate_par {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("Dep")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "+++++++++"),
      ("cba", "+++++++++"),
      ("wba", "+++++++++"),
      ("nba", "+++++++++")
    ))

    /*0.将自定义分，放在参数中，读取数据，将数据放在不同分区中*/
    val myParRdd: RDD[(String, String)] = rdd.partitionBy(new MyPartition())

    myParRdd.saveAsTextFile("output")
    sc.stop()
  }

  class MyPartition extends Partitioner {
    /*1.分区的数量*/
    override def numPartitions: Int = 3

    /*2.分区的规则
    *   根据数据的key值，返回数据的索引（从0开始）*/
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "cba" => 1
        case _ => 2
      }
    }
  }

}
