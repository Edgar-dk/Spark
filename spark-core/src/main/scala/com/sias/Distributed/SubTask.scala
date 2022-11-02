package com.sias.Distributed

/**
 * @author Edgar
 * @create 2022-11-02 20:22
 * @faction:
 */
class SubTask extends Serializable {
  var data: List[Int] = _
  var logic: (Int => Int) = _

  def compute() = {
    data.map(logic)
  }
}
