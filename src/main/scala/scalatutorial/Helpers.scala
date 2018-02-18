package scalatutorial

object Helpers {

  /**
    * Implicit class example
    *
    * @param x
    */
  implicit class IntWithTimes(x: Int) {
    def times[A](f: => A): Unit = {
      def loop(current: Int): Unit =
        if (current > 0) {
          f
          loop(current - 1)
        }
      loop(x)
    }
  }

}
