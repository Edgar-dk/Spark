package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_partitionBy {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    /*1.将list中的数据，变成key-value类型*/
    val mapRdd: RDD[(Int, Int)] = rdd.map((_, 1))
    /*2.使用的不是RDD里面的方法，是通过隐士转换的形式把数据，对应到范围内的方法
    *   可以在partitionBy参数中，填写不同的参数，从而把数据重分发到不同的分区中
    *   (2,1)
        (4,1)

        1）
        HashPartitioner对应的分区规则是，底层取模运算，奇数，在一个分区中，偶数在一个
        分区中，后面的2还是和上面的2一样，用两个分区实现
        2）
        如果，这个newRDD在用，相同参数的分区器，参数一样，分区数量一样，底层不会在执行一次
        这个方法，参数和分区有一个不一样的，都会在次执行一次*/
    val newRdd: RDD[(Int, Int)] = mapRdd.partitionBy(new HashPartitioner(2))
    newRdd.saveAsTextFile("output")
    sc.stop()
  }
}
