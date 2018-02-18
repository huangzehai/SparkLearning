package scalatutorial

object VariableArgumentsExample {
  def main(args: Array[String]): Unit = {
    printStrings("Hello", "World")
  }

  def printStrings(args: String*) = {
    var i: Int = 0;

    for (arg <- args) {
      println("Arg value[" + i + "] = " + arg);
      i = i + 1;
    }

    val it = args.iterator
    while (it.hasNext) {
      println(it.next)
    }
  }

}
