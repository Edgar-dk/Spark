package com.sias.Distributed

/**
 * @author Edgar
 * @create 2022-11-02 18:49
 * @faction:
 */
class task extends Serializable {
  val data = List(1, 2, 3, 4)
  val ni = (num: Int) => {
    num * 2
  }
  /*1.第一个Int是，传入的参数类型，第二个Int是，得到的返回值参数类型
  *   后面的是至简原则的方式书写函数，_代表传入的参数，*/
  val log: (Int => Int) = {
    _ * 2
  }

  def compute() = {
    data.map(ni)
  }
}
