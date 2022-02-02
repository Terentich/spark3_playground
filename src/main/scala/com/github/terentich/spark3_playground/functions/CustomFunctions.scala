package com.github.terentich.spark3_playground.functions

import org.apache.spark.sql.{Column, MillisToTs}

object CustomFunctions {
  def millis_to_ts(c: Column) = new Column(MillisToTs(c.expr))
}
