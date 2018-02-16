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


    //Max value by key
    val initialValue = "";
    val maxByKey = kv.aggregateByKey(initialValue)(max, max)
    println(maxByKey.collect())

    //Average value by key

    val kvList = Array("foo=1", "foo=2", "foo=3", "foo=4", "bar=5", "bar=6")
    val dataSet = sc.parallelize(kvList)
    //Create key value pairs
    val keyAndValue = dataSet.map(_.split("=")).map(v => (v(0), v(1).toInt)).cache()

    val initial = (0, 0)
    val sum = (s: (Int, Int), value: Int) => (s._1 + value, s._2 + 1)
    val sumMerge = (s1: (Int, Int), s2: (Int, Int)) => (s1._1 + s2._1, s1._2 + s2._2)
    val result = keyAndValue.aggregateByKey(initial)(sum, sumMerge)
    val avgByKey = result.map(item => (item._1, item._2._1.toDouble / item._2._2))
    println(avgByKey.collect())
  }


  private def max(a: String, b: String): String = {
    if (a == Nil) {
      return b
    }

    if (b == Nil) {
      return a
    }

    if (a.compareTo(b) > 0) {
      return a
    } else {
      return b
    }
  }

}
