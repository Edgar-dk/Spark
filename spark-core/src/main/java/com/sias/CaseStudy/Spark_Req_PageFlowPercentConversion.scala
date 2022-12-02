package com.sias.CaseStudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-01 10:36
 * @faction:
 */

/*一：页面转化率
*    */
object Spark_Req_PageFlowPercentConversion {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Req_PageFlowPercentConversion")
    val sc = new SparkContext(SparkConf)
    val actionRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    /*1.数据处理
    *   将读取到的数据，放在一个javaBean里面，*/
    val actionDataRdd = actionRdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong,
        )
      }
    )
    actionRdd.cache()

    /*2.计算分母
    *   将JavaBean数据改变数据格式，统计页面的点击数量，toMap是变成Map数据
    *   提取分母，直接把页面点击数量获取出来*/
    val pageIdToCount: RDD[(Long, Long)] = actionDataRdd.map(
      action => {
        (action.page_id, 1L)
      }
    ).reduceByKey(_ + _)
    val denominator: Map[Long, Long] = pageIdToCount.collect().toMap
    //    println(denominator.foreach(println))

    /*3.计算分子
    *   如果是直接获取page_id，数据是乱的，没有办法，把具有逻辑的数据连接在一个，先按照，sessionid（用户id）
    *   分组，然后在组内进行时间排序，在获取，页面的id，此时这个id是，有序的，并且，已经被整理成是一个用户，在
    *   按照时间顺序排序的，页面，在从前到后拉链，t是，已经被拉好链的数据，只是，在标记一下，有，数据1，为以后
    *   数据统计做铺垫，下面的代码是，统计，一个一个的用户，的页面跳转*/
    val sessionRdd: RDD[(String, Iterable[UserVisitAction])] = actionDataRdd.groupBy(_.session_id)
    val mvRdd: RDD[(String, List[((Long, Long), Int)])] = sessionRdd.mapValues(
      iter => {
        val sortList: List[UserVisitAction] = iter.toList.sortBy(_.action_time)
        val flowIds: List[Long] = sortList.map(_.page_id)
        val pageFlowIds: List[(Long, Long)] = flowIds.zip(flowIds.tail)
        pageFlowIds.map(
          t => {
            (t, 1)
          }
        )
      }
    )
    println("++++++++++++++")


    //    println(mvRdd.foreach(println))
    /*3_01，只要第二个数据，并且，把可迭代的集合去掉，一旦只要第二个数据，数据就变得混乱了
            各个用户之间的数据混在一起，然后在按照key去合并数据。key就是页面的跳转联系，前面
            按照用户和数据统计出来，是为了这个地方，做铺垫，*/
    val flatRdd: RDD[((Long, Long), Int)] = mvRdd.map(_._2).flatMap(list => list)
    println(flatRdd.foreach(println))
    val dataRdd: RDD[((Long, Long), Int)] = flatRdd.reduceByKey(_ + _)
    //    println(dataRdd.foreach(println))
    /*4.计算转化率
    *   getOrElse是看看，denominator有没有这个数据，要的话，用里面的数据，没有的话，用后面的0L
    *   计算转化率的时候，不是按照一个用户去统计的，是按照所有用户点击跳转页面的数量和计算的*/
    dataRdd.foreach {
      case ((pageid1, pageid2), sum) => {
        val long: Long = denominator.getOrElse(pageid1, 0L)
        println(s"页面${pageid1}跳转到页面${pageid2}转化率是：" + (sum.toDouble / long))
      }
    }
    sc.stop()
  }

  //用户访问动作表
  case class UserVisitAction(
                              date: String, //用户点击行为的日期
                              user_id: Long, //用户的 ID
                              session_id: String, //Session 的 ID
                              page_id: Long, //某个页面的 ID
                              action_time: String, //动作的时间点
                              search_keyword: String, //用户搜索的关键词
                              click_category_id: Long, //某一个商品品类的 ID
                              click_product_id: Long, //某一个商品的 ID
                              order_category_ids: String, //一次订单中所有品类的 ID 集合
                              order_product_ids: String, //一次订单中所有商品的 ID 集合
                              pay_category_ids: String, //一次支付中所有品类的 ID 集合
                              pay_product_ids: String, //一次支付中所有商品的 ID 集合
                              city_id: Long
                            ) //城市 id
}
