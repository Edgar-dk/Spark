package com.core.framework.common

import com.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-03 11:15
 * @faction:
 */
trait TApplication {

  /*1.里面的模板数据是固定的，只需要改变，op就可以了，至于op而言，是需要
  *   从外界传递过来的逻辑，需要放在try中，防止出现异常，对于，传递的参数
  *   而言，master，和APP传递进来，使用，传递的，不传递，使用默认的只*/
  def start(master:String="local[*]",app:String="Application")(op: => Unit): Unit = {
    val SparkConf = new SparkConf().setMaster(master).setAppName(app)
    val context = new SparkContext(SparkConf)
    EnvUtil.put(context)
    try {
      op
    } catch {
      case ex => println(ex.getMessage)
    }
    context.stop()
    /*1_01，将用完的工具类清理掉（就是把里面的数据清除掉）*/
    EnvUtil.clear()
  }

}
