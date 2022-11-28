package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_reduceByKey {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[(String,Int)] = sc.makeRDD(List(("a",1),("a",2),("a",3),("b",2)))

    /*1.
        1）将相同的key放在一个组中，在把value相加，相加过程是两两进行的（按照scala底层设计的方式）
    *   1，2，相加后，然后把得到的结果在给3相加
        2）如果是只有一个数据的话，不会相加*/
    val reduceRdd: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => (x + y))
    reduceRdd.collect().foreach(println)
    sc.stop()
  }
}
