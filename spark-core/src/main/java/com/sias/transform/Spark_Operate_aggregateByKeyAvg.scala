package com.sias.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-13 14:36
 * @faction:
 */
object Spark_Operate_aggregateByKeyAvg {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)

    // TODO 算子 —— Key-Value 类型 —— aggregateByKey & mapValues
    // TODO 获取相同key的数据的value的平均值
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 5), ("a", 3), ("b", 9),
      ("b", 4), ("b", 2), ("a", 1)
    ), 2)
    rdd.saveAsTextFile("output")


    val aggregateRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      /*1.分区内处理数据，t，是List中元组的第二个数据，v是aggregateByKey后面第二个参数
      *   0和5这个元组，次数相加，由于只有一个元组，因此就一次，所以是1，aggregateByKey第一个
      *   参数是和元组中，第二个数据相加的，得到的结果，放在aggregateByKey第一个位置上，次数放在
      *   第二个位置上，两个分区一并处理数据*/
      (t, v) => {
        val tuple1: (Int, Int) = (t._1 + v, t._2 + 1)
        tuple1
      }
      ,
      /*2.分区间处理数据，t1是第一个分区的数据，也就是part-00000，这个，t2，是part-00001，这个
          t1，t2中，相同的key进入到一个分区内
      *   然后在重复分区内的操作，得到的结果一并把数据返回，下面计算逻辑，第一个是，数值相加，第二个是
          次数相加*/
      (t1, t2) => {
        val tuple2: (Int, Int) = (t1._1 + t2._1, t1._2 + t2._2)
        tuple2
      }
    )

    /*3.计算平均数*/
    val resultRDD: RDD[(String, Int)] = aggregateRDD.mapValues {
      case (num, cnt) => {
        num / cnt
      }
    }

    resultRDD.collect().foreach(println)
    sc.stop()
  }
}
