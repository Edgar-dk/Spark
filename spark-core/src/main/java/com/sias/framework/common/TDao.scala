package com.sias.framework.common

import com.sias.framework.util.EnvUtil

/**
 * @author Edgar
 * @create 2022-12-03 11:23
 * @faction:
 */
trait TDao {
  def readFile(path:String)={
    EnvUtil.take().textFile(path)
  }
}
