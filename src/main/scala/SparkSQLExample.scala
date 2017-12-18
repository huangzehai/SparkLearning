import org.apache.spark.sql.SparkSession

object SparkSQLExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val df = spark.read.option("header", "true").csv("input/movielens/movies.csv")

    // Displays the content of the DataFrame to stdout
    df.show()
  }
}
