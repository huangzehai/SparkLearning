package scalatutorial

import scala.util.Random

object PatternMatchingExample {
  def main(args: Array[String]): Unit = {
    val x: Int = Random.nextInt(10)
    val c = x match {
      case 0 => "zero"
      case 1 => "one"
      case 2 => "two"
      case _ => "many"
    }

    println(c)

    val doubleArray = Array(1, 2, 3).map { case num => num * 2 }
    doubleArray foreach println
  }

}
