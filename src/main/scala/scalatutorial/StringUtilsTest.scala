package scalatutorial

object StringUtilsTest {
  def main(args: Array[String]): Unit = {
    import StringUtils._
    val name = "Tom"
    println($"Hi $name")
  }

}
