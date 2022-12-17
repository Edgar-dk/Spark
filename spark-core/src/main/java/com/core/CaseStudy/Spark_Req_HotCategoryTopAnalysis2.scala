package com.core.CaseStudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-01 10:36
 * @faction:
 */

/*一：热门品类分析
*    1）问题：reduceByKey多次使用*/
object Spark_Req_HotCategoryTopAnalysis2 {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Req_HotCategoryTopAnalysis")
    val sc = new SparkContext(SparkConf)
    /*1.获取数据*/
    val actionRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    /*2.改变结构化问题
        在一个方法中，筛选三个，以及用一个reduceByKey*/
    val flatMapRdd: RDD[(String, (Int, Int, Int))] = actionRdd.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != null) {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => {
            (id, (0, 1, 0))
          })
        } else if (datas(10) != null) {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    /*3.t1是一个，元组，t2是一个元组，如果出现三个的话，前两个合并，然后得到的结果
    *   在和第三个合并，
    *   注意：上面是在相同key的基础上，合并value的*/
    val analysisRDD = flatMapRdd.reduceByKey(
      (t1, t2) => {
        ( t1._1+t2._1, t1._2 + t2._2, t1._3 + t2._3 )
      }
    )
    /*5_02，排序的话，先按照第一个，在按照第二个*/
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }
}
