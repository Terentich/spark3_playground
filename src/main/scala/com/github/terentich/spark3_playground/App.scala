package com.github.terentich.spark3_playground

import com.github.terentich.spark3_playground.functions.CustomFunctions.millis_to_ts
import com.github.terentich.spark3_playground.operators.CustomerOperators
import com.github.terentich.spark3_playground.operators.CustomerOperators.Operators
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Spark 3 Application")
      .getOrCreate()

    spark.experimental.extraStrategies = CustomerOperators.strategies

    val data: Seq[(Long, String)] = Seq(
      (System.currentTimeMillis(), "")
    )

    val df = spark
      .createDataFrame(data)
      .toDF("time_in_milliseconds", "")
      .drop("")

    df
      .withColumn("timestamp_cast", col("time_in_milliseconds").cast(TimestampType))
      .withColumn("timestamp_custom_function", millis_to_ts(col("time_in_milliseconds")))
      .alreadySorted(Seq())
      .show(10, truncate = false)

    df.explain(true)

    spark.stop()
  }
}