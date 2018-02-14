import org.apache.spark.{SparkConf, SparkContext}

object Counter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Counter")
    val sc = new SparkContext(conf)
    var counter = 0
    val rdd = sc.parallelize(1 to 100)

    // Wrong: Don't do this!!
    rdd.foreach(x => counter += x)

    println("Counter value: " + counter)

    val total = rdd.reduce((a, b) => a + b)
    println("total: " + total)
  }

}
