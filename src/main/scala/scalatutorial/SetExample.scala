package scalatutorial

import scala.collection.mutable

object SetExample {
  def main(args: Array[String]): Unit = {
    val set = new mutable.HashSet[String]()
    set.add("Hi")
    set += "Tom"

    set add "Kate"

    println(set)

    var list1 = List(1)
    val list2 = mutable.LinkedList(2)
    val list3 = list1 ++: (list2)
    val list4 = list1 ++ list2
    println(list3)
    println(list4)

    list1 ++= list2
    println(list1)

  }

}
