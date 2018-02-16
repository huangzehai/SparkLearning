package scalatutorial

object AggregateExample {
  def main(args: Array[String]): Unit = {
    //    val sum = List('a', 'b', 'c').aggregate(0)({ (sum, ch) => sum + ch.toInt }, { (p1, p2) => p1 + p2 })
    val sum = List('a', 'b', 'c').aggregate(0)((sum, ch) => sum + ch.toInt, (p1, p2) => p1 + p2)
    println(sum)
  }
}
