package com.sias.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-11-28 21:06
 * @faction:
 */
object Spark_Operate_foreach {
  def main(args: Array[String]): Unit = {
    val sparkCof: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkCof)
    val rdd: RDD[Int] = sc.makeRDD(List())

    val user = new User
    //SparkException: Task not serializable
    //NotSerializableException: com.ustb.ly.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action$User

    //RDD算子中传递的函数是包含闭包操作，那么就会进行检测功能
    //闭包检测功能

    /*1.RDD方法外部都在Driver执行，对于，内部的逻辑代码是在Excuter执行
    *   这个时候，Driver，传递到Excuter，需要把数据序列化操作，因为这个
    *   是分布式节点的逻辑，对于user，传递过来之后，行成闭包，把这个变量
    *   的范围发生了变化，对于foreach里面闭包而言，对于外界传递过来数据，
    *   需要检查，变量是否序列化，没有的话，报错，根本就执行不到运行的算子
    *   直接在检查的时候，报错*/
    rdd.foreach(num => {
      println(user.age + num)
    })
    sc.stop()
  }

  //class User extends Serializable {
  //样例类在编译时，会自动混入序列化特质（实现可序列化接口）
  //case class User() {
  class User {
    var age: Int = 30
  }

}
