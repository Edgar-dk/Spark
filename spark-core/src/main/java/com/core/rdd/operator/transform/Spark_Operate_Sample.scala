package com.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_Sample {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    /*1.第三个参数，种子，确定了，每一个数据被抽取到的概率就确定了
    *   数据就唯一了
    *   第二个参数，每一个数据被抽取到的概率，在没有使用是第三个参数的时候，默认使用
    *   系统时间，作为种子。会使的，每一个抽取到的
    *   概率都是0.4，所以每次，抽取到的数据也是不一样的。2,5,6,7,8,9(第一次,在第二次会不一样)
    *   在使用了，第三个参数的时候，种子
    *   为每一个数据，根据算法设定好被抽取到的概率，概率是不一样的，只有当这个概率大于设置
    *   好的基准值（0.4），就可以被抽取出来，1,3,6,8,9
    *
    *   注意：上面是在数据不放会里面，进行分析的，使用自定义的种子（种子是1）和，默认的种子*/
    println(rdd.sample(
      false,
      0.4,
      1
    ).collect().mkString(","))

    /*2.当数据可以放进去的说，第二个参数，表示可能被抽取到的次数，
    *   可能大于这个数，也可能小于这个数（使用默认的种子），每次抽取到的数据
    *   都是不一样的
    *   1,1,2,2,2,2,4,4,4,5,5,6,6,6,8,9,10,10,10
    *
    *   使用自定义种子（种子是1），数据是一样的，
    *   1,2,2,3,3,3,3,5,5,6,6,7,7,8,8,8,8,9,9,10,10
    *   1,2,2,3,3,3,3,5,5,6,6,7,7,8,8,8,8,9,9,10,10*/
    println(rdd.sample(
      true,
      2,
//      1
    ).collect().mkString(","))
    sc.stop()
  }
}
