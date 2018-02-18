import org.apache.spark.{SparkConf, SparkContext}

object PipeExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Pipe Example")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4)
    val dataSet = sc.parallelize(data)
    val result = dataSet.pipe("cat ")
    println(result.collect())
  }

}
