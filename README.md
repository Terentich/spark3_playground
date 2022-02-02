# Spark 3 Playground
This is a simple project for experimenting with Spark 3.

## Custom Spark functions
It is possible to create your own column functions, but currently (February 2022) it is still an experimental API and you have to place your function in the package `org.apache.spark.sql`.

See a basic implementation in the file [MillisToTs.scala](src/main/scala/org/apache/spark/sql/MillisToTs.scala) and the corresponding [example](src/main/scala/com/github/terentich/spark3_playground/App.scala).

## Custom Spark Transformations
Again, here we have an experimental API and we need to create a bit more objects:
1. The logical Plan object (in our case AlreadySorted).
2. The physical Plan object (AlreadySortedExec).
3. The strategy object is responsible for converting the logical plan to the physical plan (AlreadySortedStrategy).

All these objects are located in the file [AlreadySorted.scala](src/main/scala/com/github/terentich/spark3_playground/operators/AlreadySorted.scala).

The last thing we need to do is register our strategy. This code and usage example you can find in the file [App.scala](src/main/scala/com/github/terentich/spark3_playground/App.scala).

# Resources
Used materials:

https://medium.com/@vladimir.prus/advanced-custom-operators-in-spark-79b12da61ca7
https://medium.com/@vladimir.prus/spark-partitioning-full-control-3c72cea2d74d
