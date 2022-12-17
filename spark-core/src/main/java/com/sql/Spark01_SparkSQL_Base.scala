package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author Edgar
 * @create 2022-12-07 15:56
 * @faction:
 */
object Spark01_SparkSQL_Base {
  def main(args: Array[String]): Unit = {

    /*1.增加环境对象
    *   在SparkSession，底层已经封装了SparkContent*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    /*1_01，读取数据*/
    val frame: DataFrame = spark.read.json("datas/test.json")
    frame.show()

    /*1_02，
    DataFrame => SQL
    从读取出来的数据的地方，变成视图，可以从视图中，结构化的方式，获取数据
    本质上，从文件中，读取出来的数据，就是DataFrame，*/
    frame.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    spark.sql("select age,username from user").show()
    spark.sql("select avg(age) from user").show()

    /*1_03,
    DataFrame => DSL
    所谓的DSL，就是将读取的数据，操作（+1，或者，其他的操作）
    直接写，$，或者是‘的话，不能使用这个数据变量，需要把，隐士变量
    导入进来*/
    import spark.implicits._
    frame.select("age","username").show()
    frame.select('age+1).show()
    frame.select($"age"+1).show()

    /*1_04,
      构造DataSet，本质上，DataSet底层是DataFrame，所以，DataFrame
      的功能DataSet也是可以使用的*/
    val ints = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = ints.toDS()
    ds.show()

    /*1_05，
      rdd => DataFrame*/
    /*2.关闭环境*/
    spark.close()
  }
}
