package com.sias.Distributed

/**
 * @author Edgar
 * @create 2022-11-02 18:49
 * @faction:
 */
class task1 extends Serializable {
  val data = List(1, 2, 3, 4)

  val logic: (Int => Int) = {
    _ * 2
  }
}
