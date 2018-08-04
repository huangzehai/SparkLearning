import org.apache.spark.{SparkConf, SparkContext}

object MonthlyActiveUser {
  def main(args: Array[String]) {
    val data = ("201801", 1) :: ("201801", 2) :: ("201801", 3) :: ("201803", 1) :: ("201803", 1) :: Nil
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(data)
    val result = rdd.groupByKey.map(item => (item._1, item._2.toList.distinct.size))
    result.collect.foreach(println)
    sc.stop()
  }
}
