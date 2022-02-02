package com.github.terentich.spark3_playground.operators

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.functions.col

case class AlreadySorted(sortKeys: Seq[Attribute], child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(child = newChild)
}

case class AlreadySortedExec(sortKeys: Seq[Attribute], child: SparkPlan) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override def outputOrdering: Seq[SortOrder] = sortKeys.map(SortOrder(_, Ascending))

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = copy(child = newChild)
}

object AlreadySortedStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[AlreadySortedExec] = plan match {
    case AlreadySorted(sortKeys, child) =>
      AlreadySortedExec(sortKeys, planLater(child)) :: Nil
    case _ => Nil
  }

  def alreadySorted(columnNames: Seq[String])(implicit df: DataFrame): DataFrame = {
    val columns = columnNames.map(
      col(_).expr.asInstanceOf[Attribute])
    val plan = AlreadySorted(columns, df.queryExecution.analyzed)
    val encoder = RowEncoder(df.schema)
    new DataFrame(df.sparkSession, plan, encoder)
  }
}
