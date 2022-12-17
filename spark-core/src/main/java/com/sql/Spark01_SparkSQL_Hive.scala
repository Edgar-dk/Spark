package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

/**
 * @author Edgar
 * @create 2022-12-07 15:56
 * @faction:
 */

/*UDF函数，定义强类型的*/
object Spark01_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {

    /*1.增加环境对象
    *   在SparkSession，底层已经封装了SparkContent
    *   enableHiveSupport是对hive的操作
    *   在pom中，添加上，hive的依赖，然后启动main线程，连接hive
    *   在连接hive的时候，使用到Hadoop，在hosts位置，匹配域名的IP地址，访问Linux上的服务器
    *   其实本质上，还是连接hive底层的MySQL以及HDFS的存储数据*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()


    spark.sql("show tables").show()
    spark.close()
  }
}
