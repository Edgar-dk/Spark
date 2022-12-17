package com.core.framework.application

import com.core.framework.common.TApplication
import com.core.framework.controller.WordCountController
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author Edgar
 * @create 2022-12-03 10:58
 * @faction:
 */
object WordCountApplication extends App with TApplication {


  /*1.这个环境，是固定的，因此，把这个固定的只，封装成一个特质，使用的时候，直接从特质中，得到*/
  start(){
    val controller = new WordCountController
    controller.dispatch()
  }

}
