package org.apache.spark.sql.example

import org.apache.spark.sql.SparkSession

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from MQTT Server.
 *
 * Usage: MQTTStreamWordCount <brokerUrl> <topic>
 * <brokerUrl> and <topic> describe the MQTT server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, a MQTT Server should be up and running.
 *
 */
object MQTTStreamWordCount  {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: MQTTStreamWordCount <brokerUrl> <topic>") // scalastyle:off println
      System.exit(1)
    }

    val brokerUrl = args(0)
    val topic = args(1)

    val spark = SparkSession
      .builder
      .appName("MQTTStreamWordCount")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to mqtt server
    val lines = spark.readStream
      .format("mqtt")
      .option("topic", topic).option("persistence", "memory")
      .load(brokerUrl).selectExpr("CAST(payload AS STRING)").as[String]

    // Split the lines into words
    val words = lines.flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}

