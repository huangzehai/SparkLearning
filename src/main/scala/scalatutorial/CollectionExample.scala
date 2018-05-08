package scalatutorial

object CollectionExample {
  def main(args: Array[String]): Unit = {
    val arr = Array(1, 2, 3)
    val doubleArray = arr.map {
      case num =>
        num * 2
    }

    val a = doubleArray.map(_ + 1)

    a foreach println
  }

}
