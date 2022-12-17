package com.core.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    // 创建 Spark 运行配置对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    // 创建 Spark 上下文环境对象（连接对象）
    val sc = new SparkContext(sparkConf)

    val lines: RDD[String] = sc.textFile("datas")


    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)

    val res: RDD[(String, Int)] = wordGroup.map {
      case (str, list) =>
        val tuple1: (String, Int) = {
          /*1.reduce是用来计算，用来统计数据用的一个方法*/
          val tuple: (String, Int) = list.reduce {
            (t1, t2) => {
              /*01.t1._1是第一个数据，也就是字符串，后面的是把第一个元组中的第二个数据，和第二个元组中的第二个数据合并
              *    然后在赋值给一个新的元组，然后这个新的元组在重新和下一个相同字符串的Hello进行合并，然后在合并成一个新的
              *    元组，然后在和下一个相同的字符串元组合并，直到下一个字符串不一样，才停止合并，然后不一样的元组，在重复
              *    上述的步骤，最后把这些元组信息放在一个列表中*/
              val tuple2: (String, Int) = (t1._1, t1._2 + t2._2)
              tuple2
            }
          }
          tuple
        }
        tuple1
    }

    val array: Array[(String, Int)] = res.collect()
    array.foreach(println)

    //TODO　关闭 Spark 连接
    sc.stop()
  }
}