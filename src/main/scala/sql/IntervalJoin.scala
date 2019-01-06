package sql

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.expressions.{Alias, EqualTo}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Range}
import org.apache.spark.sql.execution.{ProjectExec, RangeExec, SparkPlan}


object IntervalJoin extends Strategy with Serializable {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case Join(Range(start1, end1, 1, part1, Seq(o1), isStreaming1),
    Range(start2, end2, 1, part2, Seq(o2), isStreaming2),
    Inner, Some(EqualTo(e1, e2)))
      if ((o1 semanticEquals e1) && (o2 semanticEquals e2)) ||
        ((o1 semanticEquals e2) && (o2 semanticEquals e1)) =>

      if ((end2 >= start1) && (end2 <= end2)) {
        //start of the intersection
        val start = math.max(start1, start2)
        // end of the intersection
        val end = math.min(end1, end2)
        val part = Math.max(part1.getOrElse(200), part2.getOrElse(200))
        //create a new range to represent the intersection
        val result = RangeExec(Range(start, end, 1, Some(part), o1 :: Nil, isStreaming1))
        val twoColumns = ProjectExec(
          Alias(o1, o1.name)(exprId = o1.exprId) :: Nil, result)
        twoColumns :: Nil
      } else {
        Nil
      }
    case _ => Nil
  }
}
