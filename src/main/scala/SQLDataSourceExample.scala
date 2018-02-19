import org.apache.spark.sql.SparkSession

object SQLDataSourceExample {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val usersDF = spark.read.load("input/users.parquet")
    usersDF.select("name", "favorite_color").write.save("output/namesAndFavColors.parquet")
  }
}
