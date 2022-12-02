package com.sias.CaseStudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-01 10:36
 * @faction:
 */

/*一：热门品类分析，前10个商品中，每一个商品，前10用户的点击数量
*    */
object Spark_Req_HotCategoryTopAnalysisSession {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Req_HotCategoryTopAnalysisSession")
    val sc = new SparkContext(SparkConf)
    val actionRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRdd.cache()
    /*1.只需要看，是否在前十里面，所以，只用一个就可以了*/
    val top10: Array[(String)] = top10Category1(actionRdd)

    /*2.过滤原始数据，保留点击的，其他的不要
    *   从原始数据中筛选的，为什么不在已经筛选好的数据中，在筛选
    *   因为这么多的数据，虽然知道是第一个，但是用户不知道，目的是
    *   为了筛选用户，下面重要的一步就是，在前10里面筛选数据*/
    val filterActionRdd: RDD[String] = actionRdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10.contains(datas(6))
        } else {
          false
        }
      }
    )
    /*3.品类id，以及sessionid点击的统计，统计之后，在做，合并*/
    val reduceRdd: RDD[((String, String), Int)] = filterActionRdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    /*4.将统计的结果结构上的转换*/
    val mapRdd: RDD[(String, (String, Int))] = reduceRdd.map {
      case ((cid, sid), sum) => {
        (cid, (sid, sum))
      }
    }

    /*5.相同品类进行分组*/
    val groupRdd: RDD[(String, Iterable[(String, Int)])] = mapRdd.groupByKey()

    /*6.分组后的数据进行点击量排序，取前10名
    *   toList，变成，list结构的数据，sortBy，按照第二个排序
    *   sortBy默认按照升序，1，2，3后面那个参数，按照降序，*/
    val resultRdd: RDD[(String, List[(String, Int)])] = groupRdd.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )
    resultRdd.collect().foreach(println)

    sc.stop()
  }

  def top10Category1(actionRdd: RDD[String]) = {
    val flatMapRdd: RDD[(String, (Int, Int, Int))] = actionRdd.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => {
            (id, (0, 1, 0))
          })
        } else if (datas(10) != "null") {
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    val analysisRDD = flatMapRdd.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)
  }
}
