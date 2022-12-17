package com.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author Edgar
 * @create 2022-11-28 21:06
 * @faction:
 */


/*一.自定义累加器
*    使用自定义的累加器，实现，WordCount*/
object Spark_Operate_UserDefine {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Spark", "Hello"))

    /*1.皆为对象，因此自定义累加器的时候，需要在类里面，使用的话，在去创建这个类的对象*/
    val accumulator = new MyAccumulator()

    /*01：注册累加器，第一个参数，是注册自定义的，第二个参数，累加器的名字*/
    sc.register(accumulator, "WordCount")
    rdd.foreach(
      word => {
        accumulator.add(word)
      }
    )
    println(accumulator.value)
    sc.stop()
  }

  /*2.自定义的话，需要继承AccumulatorV2，泛型中，第一个参数，数据输入类型，第二个是，数据输出类型*/
  class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    /*00.接收WordCount
    *    是一个空的集合*/
    private val wcMap: mutable.Map[String, Long] = mutable.Map[String, Long]()


    /*01.判断是否是初始状态
    *    如果是空的，就是初始状态*/
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    /*02.赋值一个新的累加器*/
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAccumulator
    }

    /*03.重置累加器（也就是清空里面的数据）*/
    override def reset(): Unit = {
      wcMap.clear()
    }

    /*01.获取累加器需要计算的值*/
    override def add(word: String): Unit = {
      /*0001.看看里面有没有单词，Word是之前的，有的话，+1，没有的话，是0L*/
      var newMap = wcMap.getOrElse(word, 0L) + 1
      /*0002.更新数据，将新的数据，放在wcMap中（放在这个集合中）*/
      wcMap.update(word, newMap)
    }

    /*02.Driver端合并多个累加器，是两个Map合并*/
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1: mutable.Map[String, Long] = this.wcMap
      val map2: mutable.Map[String, Long] = other.value
      /*001.遍历map2的方式去判断map1里面的数据，是有还是没有，
      *     有的话，把map2的数据加在map1中，没有的话，新建一个
      *     然后在更新map1*/
      map2.foreach {
        case (word, count) => {
          var newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    /*03.累加器的结果*/
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
