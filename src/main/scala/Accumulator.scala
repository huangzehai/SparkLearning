import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator Example")
    val sc = new SparkContext(conf)
    val counter = sc.longAccumulator("account")
    val rdd = sc.parallelize(1 to 100)
    //foreach is Spark Action
    rdd.foreach(item => counter.add(item))
    println("counter: " + counter.value)


    //map is Spark Transformation
    rdd.map { item =>
      counter.add(item)
      item
    }

    println("counter2: " + counter.value)
  }

}
