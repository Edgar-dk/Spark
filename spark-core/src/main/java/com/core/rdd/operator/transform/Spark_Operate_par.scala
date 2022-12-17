package com.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_par {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    // TODO 算子 —— map
    /*0.在去处理转换流的方式的时候，效率不高，过来一个数据，处理一个数据
    *   然后在把数据释放掉，这种效率不高，可以使用缓冲流的形式，把数据缓冲
    *   多少后，在处理，在释放
    *   注意：这个mapPartitions是把一个分区内的数据都拿到之后，在去做逻辑的处理的
    *   这个时候，有一个问题，首先，这个是两个分区，分区的数据，对应到核数，也就是
    *   两个线程去处理这个数据。一个线程中有这个rdd.mapPartitions，方法，另外一个线程
    *   中，也有这个方法，因为多线程是同时并行的执行（电脑核数多），把数据得到后，在做统一的
    *   处理，处理完后的数据不会被释放，还存在对象的引用，为什么不被释放，
    *   把集合拿过来，去引用这些数据，没有吧所有的数据处理完，引用就依然存在，所以引用还存在（就是内存
    *   分区数据还存在）*/

    //mapPartitions:可以以分区为单位进行数据转换操作
    //              但是会将整个分区的数据加载到内存中进行引用
    //              如果处理完的数据是不会被释放掉的，存在对象的引用
    //              在内存较小，数据量较大的场合下，容易出现内存溢出
    /*1.在去处理map的话，内存中的数据，处理一个，把数据释放掉一个*/
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val mpRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>")
        iter.map(_ * 2)
      }
    )
    mpRDD.collect().foreach(println)
    sc.stop()
  }
}
