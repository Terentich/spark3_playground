package com.github.terentich.spark3_playground.operators

import org.apache.spark.sql.DataFrame

object CustomerOperators {
  val strategies = Seq(AlreadySortedStrategy)

  implicit class Operators(df: DataFrame) {
    def alreadySorted(columnNames: Seq[String]): DataFrame = AlreadySortedStrategy.alreadySorted(columnNames)(df)
  }
}