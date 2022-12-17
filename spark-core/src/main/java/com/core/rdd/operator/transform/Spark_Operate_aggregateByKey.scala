package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_aggregateByKey {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)

    //    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    // TODO 算子 —— Key-Value 类型 —— aggregateByKey
    //  取出每个分区内相同 key 的最大值然后分区间相加
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("a", 4)
    ), 2)
    //(a,[1,2]),(a,[3,4]) => (a,2),(a,4) => (a,6)
    //aggregateByKey存在函数柯里化，有两个参数列表
    //第一个参数列表，需要传递一个参数，表示为初始值
    //    主要用于碰见第一个key的时候，和value进行分区内计算，初始值和分区内的数据比较，谁大的谁留下，留下的在和下一个分区内的数据比较
    //第二个参数列表需要传递两个参数
    //    第一个参数表示分区内计算规则
    //    第二个参数表示分区间计算规则
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => (x + y)
    )
    println(aggRDD.collect().mkString(","))
    sc.stop()
  }
}
