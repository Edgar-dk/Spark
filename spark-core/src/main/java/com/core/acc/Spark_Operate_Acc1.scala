package com.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-28 21:06
 * @faction:
 */
/*1.累加器的使用*/
object Spark_Operate_Acc1 {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    rdd.foreach(
      num=>{
        sumAcc.add(num)
      }
    )
    println(sumAcc.value)
    sc.stop()
  }
}
