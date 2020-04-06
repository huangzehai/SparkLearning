package batch.model

import scala.collection.mutable

object MapExample {
  def main(args: Array[String]): Unit = {
    val map = scala.collection.mutable.Map[Int, String]()
    map(1) = "one"
    println(map)

    val conf = () -> new mutable.HashMap[Int, String]()
  }
}

class Config {
  def getMap(): scala.collection.mutable.Map[Int, String] = {
    new mutable.HashMap[Int, String]()
  }
}
