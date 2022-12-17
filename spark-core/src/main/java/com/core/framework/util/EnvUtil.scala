package com.core.framework.util

import org.apache.spark.SparkContext

/**
 * @author Edgar
 * @create 2022-12-03 11:30
 * @faction:
 */
object EnvUtil {
  /*0.将这个SparkContext放在一个线程池里面，相当于一个公共变量，取数据的时候
  *   也是很方便的。目的是为了共享数据，把一个数据，放在一个线程池里面的空间中
  *   可以读，可以清理，在多线程里面使用很多*/
  private val scLocal = new ThreadLocal[SparkContext]
  /*1.放数据*/
  def put(sc:SparkContext):Unit={
    scLocal.set(sc)
  }

  /*2.取数据*/
  def take():SparkContext={
    scLocal.get()
  }

  /*3.清空数据*/
  def clear():Unit={
    scLocal.remove()
  }
}
