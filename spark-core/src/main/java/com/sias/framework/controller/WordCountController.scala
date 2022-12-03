package com.sias.framework.controller

import com.sias.framework.common.TController
import com.sias.framework.service.WordFCountService
import org.apache.spark.rdd.RDD
import org.springframework.stereotype.Controller

/**
 * @author Edgar
 * @create 2022-12-03 10:53
 * @faction:
 */
class WordCountController extends TController{
  private val service = new WordFCountService

  /*1.调度
  *   对于继承的TController，而言，是一个外部的接口，
  *   以后，在去写，Controller的时候，继承这个接口，
  *   按照里面的逻辑去写，这样的话，更加的规范*/
  def dispatch(): Unit = {
    val tuples: Array[(String, Int)] = service.dataAnalysis()
    tuples.foreach(println)
  }
}
