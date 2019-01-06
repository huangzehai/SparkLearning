package sql

import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

object MultiplyOptimizationRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left, right) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Long] == 1 =>
      println("optimization of one applied")
      left
  }
}