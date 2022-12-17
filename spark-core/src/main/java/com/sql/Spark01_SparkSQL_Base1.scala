package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author Edgar
 * @create 2022-12-07 15:56
 * @faction:
 */
object Spark01_SparkSQL_Base1 {
  def main(args: Array[String]): Unit = {

    /*1.增加环境对象
    *   在SparkSession，底层已经封装了SparkContent*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/test.json")
    df.createOrReplaceTempView("user")

    /*1.自定义函数：这个功能是给字段前面加上别名*/
    spark.udf.register("other",(name:String)=>{
      "Name:"+name
    })
    spark.sql("select age,Other(username) from user").show()

    /*2.关闭环境*/
    spark.close()
  }
}
