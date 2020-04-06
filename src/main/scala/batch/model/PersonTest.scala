package batch.model

object PersonTest {
  def main(args: Array[String]): Unit = {
    val person = new Person("kate", 23)
    println(person.getName)
  }

}
