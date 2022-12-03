package com.sias.framework.service

import com.sias.framework.common.TService
import com.sias.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import javax.annotation.Resource

/**
 * @author Edgar
 * @create 2022-12-03 10:53
 * @faction:
 */
class WordFCountService extends TService{
  private val dao = new WordCountDao

  /*1.数据分析*/
  def dataAnalysis()={

    val line: RDD[String] = dao.readFile("datas/1.txt")
    val value: RDD[String] = line.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = value.groupBy(word => word)
    val value1: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }


    val tuples: Array[(String, Int)] = value1.collect()
    tuples
  }
}
