/**
 * @author Edgar
 * @create 2022-09-30 17:57
 * @faction:
 */
object Test {
  def main(args: Array[String]): Unit = {
    val ids = List[Long](1, 2, 3, 4, 5, 6, 7)
    val okflowIds: List[(Long, Long)] = ids.zip(ids.tail)
    println(okflowIds)
  }
}
