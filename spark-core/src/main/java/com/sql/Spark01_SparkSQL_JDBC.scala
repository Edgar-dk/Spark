package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author Edgar
 * @create 2022-12-07 15:56
 * @faction:
 */

/*UDF函数，定义强类型的*/
object Spark01_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {

    /*1.增加环境对象
    *   在SparkSession，底层已经封装了SparkContent*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.format("JDBC")
      .option("url", "jdbc:mysql://hadoop102:3306/Operation")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "root")
      .option("dbtable", "student")
      .load()
    df.show()
    /*2.关闭环境*/
    spark.close()
  }
}
