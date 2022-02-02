package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{AbstractDataType, DataType, LongType, TimestampType}

case class MillisToTs(millis: Expression)
  extends UnaryExpression with ExpectsInputTypes {

  override def child: Expression = millis

  override def inputTypes: Seq[AbstractDataType] = Seq(LongType)

  override def dataType: DataType = TimestampType

  override protected def doGenCode(ctx: CodegenContext,
                                   ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, c => s"$c * 1000")
  }

  override protected def nullSafeEval(timestamp: Any): Any = {
    timestamp.asInstanceOf[Long] * 1000
  }

  override def prettyName: String = "millis_to_ts"

  override protected def withNewChildInternal(newChildren: Expression): Expression =
    copy(newChildren)
}

