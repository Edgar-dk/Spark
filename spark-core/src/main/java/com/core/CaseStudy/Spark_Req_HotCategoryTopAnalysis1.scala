package com.core.CaseStudy

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-01 10:36
 * @faction:
 */

/*一：热门品类分析
*    1）问题：actionRDD重复使用:cogroup性能可能较低*/
object Spark_Req_HotCategoryTopAnalysis1 {
  def main(args: Array[String]): Unit = {
    val SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Req_HotCategoryTopAnalysis")
    val sc = new SparkContext(SparkConf)
    /*1.获取数据*/
    val actionRdd: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    actionRdd.cache()
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
    /*5_01，采取位置格式的方式使用，将获取到的数据，放在特定的位置上*/
    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRdd.map {
      case (cid, cnt) => {
        (cid, (cnt, 0, 0))
      }
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRdd.map {
      case (cid, cnt) => {
        (cid, (0, cnt, 0))
      }
    }
    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => {
        (cid, (0, 0, cnt))
      }
    }
    /*5_02，将rdd1,rdd2,rdd3连接在一个，只是一个普通的连接，就是，放在一起*/
    val sourceRdd: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    println(sourceRdd)
    println(sourceRdd.foreach(println))
    /*5_03，放在一个的数据，按照key进行value相加
    *       t1，是一个value的值，t2也是*/
    val analySisRdd: RDD[(String, (Int, Int, Int))] = sourceRdd.reduceByKey(
      (t1, t2) => {
        println("+++++++++++++++++++++")
        println(t1)
        println(t2)
        println("_____________________")
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    println(analySisRdd)
    println(analySisRdd.foreach(println))
    /*5_02，排序的话，先按照第一个，在按照第二个*/
    val resultRDD = analySisRdd.sortBy(_._2, false).take(10)

    // 6. 将结果采集到控制台打印出来
    resultRDD.foreach(println)
    sc.stop()
  }
}
