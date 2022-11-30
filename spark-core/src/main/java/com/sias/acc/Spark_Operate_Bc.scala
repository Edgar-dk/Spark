package com.sias.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Edgar
 * @create 2022-11-28 21:06
 * @faction:
 */


/*一.广播变量
*    实现类似于join的操作*/
object Spark_Operate_Bc {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val map1: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))
    /*1.封装广播变量，如不封装的话，数据map1在map函数中，容易行成闭包，在Driver端，分配数据分发策略的时候
    *   到Executor。行成多个Task的时候，闭包的数据重复出现在Task，小数量还好，大数据量的时候，出现重复的
    *   数据，不利于执行，可以把数据放在广播变量中（Executor端共享变量地址）*/
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map1)
    rdd1.map {
      case (w, c) => {
        val i: Int = bc.value.getOrElse(w, 0)
        (w, (c, i))
      }
    }.collect().foreach(println)
    sc.stop()
  }
}
