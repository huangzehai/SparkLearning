package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object RangeJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName)
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:/tmp/spark-events")
      .getOrCreate()
//    spark.sqlContext.experimental.extraStrategies = IntervalJoin :: Nil

    import spark.implicits._
    val tableA = spark.range(20000000).as('a)
    val tableB = spark.range(10000000).as('b)
    val result = tableA.join(tableB, $"a.id" === $"b.id")
      .groupBy()
      .count()
    result.show()
    result.explain()
  }

}
