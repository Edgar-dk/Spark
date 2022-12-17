package com.core.CaseStudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-01 10:36
 * @faction:
 */

/*一：热门品类分析
*     方案一*/
object Spark_Req_HotCategoryTopAnalysis {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Req_HotCategoryTopAnalysis")
    val sc = new SparkContext(SparkConf)
    /*1.获取数据*/
    val actionRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    /*2.统计品类的点击数量
    *   action是每一行数据，一行一行的处理，处理后在交给一个变量，然后在往下面执行，在换一种方式处理*/
    val clickRdd: RDD[String] = actionRdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        /*01.筛选数据，将，不是-1的筛选出来（不是-1的，表示，这个是点击的）*/
        datas(6) != "-1"
      }
    )

    /*2_01，map可以改变数据的多少（按照规则）*/
    val clickCountRdd: RDD[(String, Int)] = clickRdd.map(
      action => {
        val datas: Array[String] = action.split("_")
        /*01.将这个数据返回出去，给reduceByKey，按照key，把value相加*/
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    /*3.统计品类下单数量
    *   这个下单行为，是把商品，放在购物车里面，然后在下单，下单的时候，可以是一个
    *   也可以是多个*/
    val orderActionRdd: RDD[String] = actionRdd.filter(
      action => {
        val datas: Array[String] = action.split("_")
        /*01.筛选数据，将，不是下单的筛选出来*/
        datas(8) != "null"
      }
    )
    /*3_01，多个拆分成一个，用扁平化，操作去实现
    *       注意：这个id是商品的id，*/
    val orderCountRdd: RDD[(String, Int)] = orderActionRdd.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => {
          (id, 1)
        })
      }
    ).reduceByKey(_ + _)


    // 4. 统计品类的支付数量：（品类ID，支付数量）
    //    原理也是和订单的一样，将多个支付单号，分割开去统计
    val payActionRDD = actionRdd.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )
    // orderid => 1,2,3
    // 【(1,1)，(2,1)，(3,1)】
    val payCountRDD = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    /*5.数据聚合
    *   先分组，在聚合，按照id分组，id就是商品的id，所谓的聚合就是把这三个点击，订单，以及下单
    *   放在一个组里面*/
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
      clickCountRdd.cogroup(orderCountRdd, payCountRDD)

    println(cogroupRDD.foreach(println))
    println(cogroupRDD)
    //    cogroupRDD.collect().foreach(println)
    /*5_01，下面将可迭代的集合，变成Int类型的数据
    *       下面的hasNext，判断下一个还有没有，有话，话，true，把下一个的上一个数据
    *       取出来，如果本来就是一个的话，直接把这个取出来*/
    val analysisRDD = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {

        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    /*5_02，排序的话，先按照第一个，在按照第二个*/
    val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }
}
