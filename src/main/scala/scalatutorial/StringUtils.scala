package scalatutorial

object StringUtils {

  implicit class StringToColumn(val sc: StringContext) {
    def $(args: Any*): String = {
      val strings = sc.parts.iterator
      val expressions = args.iterator
      val buf = new StringBuffer(strings.next)
      while (strings.hasNext) {
        buf append expressions.next
        buf append strings.next
      }
      buf.toString
    }
  }

}
