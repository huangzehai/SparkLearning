import org.apache.spark.sql.SparkSession

object SaveModeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()
    import spark.implicits._


    (1 to 5).toDS().write.save("output/numbers.parquet")
    (3 to 6).toDS().write.mode("append").save("output/numbers.parquet")

    spark.read.load("output/numbers.parquet").show

  }

}
