package org.apache.spark.sql.mqtt.example;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from MQTT Server.
 *
 * Usage: JavaMQTTStreamWordCount <brokerUrl> <topic>
 * <brokerUrl> and <topic> describe the MQTT server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, a MQTT Server should be up and running.
 *
 */
public final class JavaMQTTStreamWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaMQTTStreamWordCount <brokerUrl> <topic>");
            System.exit(1);
        }

        if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
            Logger.getRootLogger().setLevel(Level.WARN);
        }

        String brokerUrl = args[0];
        String topic = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("JavaMQTTStreamWordCount");

        // check Spark configuration for master URL, set it to local if not configured
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[4]");
        }

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to mqtt server
        Dataset<String> lines = spark
                .readStream()
                .format("mqtt")
                .option("topic", topic)
                .load(brokerUrl).selectExpr("CAST(payload AS STRING)").as(Encoders.STRING());

        // Split the lines into words
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        }, Encoders.STRING());

        // Generate running word count
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start running the query that prints the running counts to the console
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();
    }
}
