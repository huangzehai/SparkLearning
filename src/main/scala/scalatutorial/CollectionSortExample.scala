package scalatutorial

import scala.util.Sorting

object CollectionSortExample {
  def main(args: Array[String]): Unit = {
    sorted
    sortWith
    sortBy
    tupleSortBy
    ordering
  }

  private def sorted = {
    val list = List(1, 3, 4, 6, 5, 2)
    val sortedList = list.sorted
    println(s"sorted: ${sortedList}")
  }


  private def sortWith = {
    val list = List(1, 3, 4, 6, 5, 2)
    val sortedList = list.sortWith((a, b) => a - b < 0)
    println(s"sortWith: ${sortedList}")
  }

  private def sortBy = {
    val list = List(1, 3, 4, 6, 5, 2)
    val sortedList = list.sortBy(a => a)
    println(s"sortBy: ${sortedList}")
  }

  private def tupleSortBy = {
    val list = List((1, 2), (5, 6), (3, 4))
    val sortedList = list.sortBy(_._1)
    println(s"sortBy: ${sortedList}")
  }

  private def ordering = {
    val pairs = Array(("a", 5, 2), ("c", 3, 1), ("b", 1, 3))
    // sort by 2nd element
    println(" sort by 2nd element")
    Sorting.quickSort(pairs)(Ordering.by[(String, Int, Int), Int](_._2))
    //    Sorting.quickSort(pairs)(Ordering.by(_._2))
    pairs foreach println
    // sort by the 3rd element, then 1st
    Sorting.quickSort(pairs)(Ordering[(Int, String)].on(x => (x._3, x._1)))
    println("sort by the 3rd element, then 1st")
    pairs foreach println
  }
}
