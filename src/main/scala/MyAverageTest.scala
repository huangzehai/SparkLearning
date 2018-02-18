import org.apache.spark.sql.SparkSession

/**
  * Spark UDF myAverage
  */
object MyAverageTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    // Register the function to access it
    spark.udf.register("myAverage", MyAverage)

    val df = spark.read.json("input/employees.json")
    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // +-------+------+

    val result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
    // +--------------+
    // |average_salary|
    // +--------------+
    // |        3750.0|
    // +--------------+

    val avg = result.first().getDouble(0)
    println(avg)
    //3750.0
  }


}
