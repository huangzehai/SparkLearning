import org.apache.spark.{SparkConf, SparkContext}

object AggregationByKeyExample {
  def main(args: Array[String]): Unit = {
    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
    val conf = new SparkConf().setAppName("Accumulator Example")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(keysWithValuesList)
    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    import scala.collection._
    //collecting unique elements per key
    val initialSet = mutable.HashSet.empty[String]
    //This function combines/merges values within a partition.
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    //This step merges values across partitions.
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)
    println(uniqueByKey.collect())


    // AggregationByKey example 2
    val initialCount = 0;
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2
    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)
    println(countByKey.collect())
  }

}
