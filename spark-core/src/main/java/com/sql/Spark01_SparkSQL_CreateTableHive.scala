package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * @author Edgar
 * @create 2022-12-07 15:56
 * @faction:
 */

/*UDF函数，定义强类型的*/
object Spark01_SparkSQL_CreateTableHive {
  def main(args: Array[String]): Unit = {

    /*1.增加环境对象
    *   在SparkSession，底层已经封装了SparkContent
    *   enableHiveSupport是对hive的操作
    *   在pom中，添加上，hive的依赖，然后启动main线程，连接hive
    *   在连接hive的时候，使用到Hadoop，在hosts位置，匹配域名的IP地址，访问Linux上的服务器
    *   其实本质上，还是连接hive底层的MySQL以及HDFS的存储数据*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use sias")

    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        | `date` string,
        | `user_id` bigint,
        | `session_id` string,
        | `page_id` bigint,
        | `action_time` string,
        | `search_keyword` string,
        | `click_category_id` bigint,
        | `click_product_id` bigint,
        | `order_category_ids` string,
        | `order_product_ids` string,
        | `pay_category_ids` string,
        | `pay_product_ids` string,
        | `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'datas/user_visit_action.txt' into table
        |sias.user_visit_action
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        | `product_id` bigint,
        | `product_name` string,
        | `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/product_info.txt' into sias.table product_info
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE `city_info`(
        | `city_id` bigint,
        | `city_name` string,
        | `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'datas/city_info.txt' into table sias.city_info
        |""".stripMargin)
    spark.sql(
      """
        |select * from city_info
        |""".stripMargin).show()
    spark.close()
  }
}
