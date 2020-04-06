package batch.model

class Person(name: String, age: Int) {

  def this(name: String) = {
    this(name, 0)
  }

  private[model] def this(age: Int) {
    this(null, age)
  }

  override def toString: String = s"Person{name: $name, age: $age}"

  def getName: String = name
}

object Person {
  def main(args: Array[String]): Unit = {

    val tom = new Person("Tom")
    val kate = new Person("Kate", 22)
    val nobody = new Person(30)
    println(s"Tom: $tom")
    println(s"Kate: $kate")
    println(s"nobody: $nobody")
  }

}
