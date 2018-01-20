import org.apache.spark.sql.SparkSession


object WordCount2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val textFile = spark.read.textFile("input/example.txt")
    val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(digitOrWord).count()
    wordCounts.show()
  }

  def digitOrWord(x: String): String = {
    if (x forall Character.isDigit) "Digit" else "Word"
  }

}
