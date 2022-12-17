package com.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @author Edgar
 * @create 2022-12-07 15:56
 * @faction:
 */

/*UDF函数，定义强类型的*/
object Spark01_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {

    /*1.增加环境对象
    *   在SparkSession，底层已经封装了SparkContent*/
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/test.json")
    df.createOrReplaceTempView("user")

    /*1.自定义函数：这个功能是给字段前面加上别名
    *   使用的functions本质上是弱类型的，将强类型的转换成弱类型的*/
    spark.udf.register("Avg",functions.udaf(new MyAvgUDF()))
    spark.sql("select Avg(age) from user").show()

    /*2.关闭环境*/
    spark.close()
  }

  /*3.自定义聚合函数，计算年龄的平均值
  *   IN：输入的数据类型Long
  *   BUF：中间计算临时的数据类型
  *   OUT：输出的数据类型Long*/
  case class Buff(var total: Long, var count: Long)

  class MyAvgUDF extends Aggregator[Long, Buff, Long] {
    /*01.初始化*/
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    /*02.输入的数据，操作buff*/
    override def reduce(buff: Buff, in: Long): Buff = {
      /*02.年龄增加
      *    后面那个buff.total是旧数据，+的in，是输入的数据
      *    后面那个buff.count也是旧数据，*/
      buff.total = buff.total + in
      buff.count = buff.count + 1
      buff
    }

    /*合并缓冲区*/
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total = buff1.total + buff2.total
      buff1.count = buff1.count + buff2.count
      buff1
    }

    /*计算*/
    override def finish(buff: Buff): Long = {
      buff.total / buff.count
    }

    /*缓冲区编码：因为这些要在网络中传输，所以要编码和解码*/
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    /*缓冲区解码：这些都是自定义的*/
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

}
