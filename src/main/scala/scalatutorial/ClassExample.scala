package scalatutorial

object ClassExample {
  def main(args: Array[String]): Unit = {
    val user = new User("Admin", 20);
    val user2 = new User("Admin", 20);
    println(user == user2)
    val person = Person("Admin", 20);
    val person2 = Person("Admin", 20);
    println(person == person2)
  }

}
