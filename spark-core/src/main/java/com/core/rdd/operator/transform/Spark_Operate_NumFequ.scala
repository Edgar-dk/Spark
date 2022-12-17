package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_NumFequ {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
      /*1.分析：这个时候，没有设置分区的数据，默认的话，是系统最大线程数目
      *   这个rdd.mapPartitionsWithIndex被执行了16次，就是16个线程
      *   执行的，每次执行得到的index排列的结果都不一样，这个是线程的执行的
      *   时间不一样，线程执行快慢不一样，得到的结果也就不一样，数据输出的顺序
      *   也是不一样的，index是分区数，分区数输出的每次都不一样，iter是分区里面
      *   的数据，也就是一个迭代器，尽管里面没有数据，也会输出一个迭代器，这些数据
      *   输出的，是综合的形式分析，其实，在每次线程执行的时候，数据和分区数已经对应好
      *   了，一个线程index只有一个数，然后到达，num的时候，这个是数据，只有，有数据
      *   的迭代器才把数据输出，没有的话，不组合数据，不输出数据，最后把带有数据的元组
      *   输出*/
      (index, iter) => {
        var i:Int =0
        i+=1
        println("index的数据",index,i)
        println("iter数据",iter)
        iter.map(
          num => {
            println("num数据",num)
            (index, num)
          }
        )
      }
    )
    mapRDD.collect().foreach(println)
    sc.stop()
  }
}
