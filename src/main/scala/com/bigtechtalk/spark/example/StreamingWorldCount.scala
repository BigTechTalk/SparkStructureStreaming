package com.bigtechtalk.spark.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StreamingWorldCount {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkWorldCount")
      .config("spark.sql.shuffle.partitions",3)
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    // Source
    val msg = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()

    // Transformation
    val processedDF = msg.selectExpr("explode(split(cast(value as string),' ')) as words")
    val countDF = processedDF.groupBy("words").count()

    // Sink
    countDF.writeStream.trigger(Trigger.ProcessingTime("2 seconds"))
      .outputMode("complete")
      .format("console")
      .start().awaitTermination()
  }
}
