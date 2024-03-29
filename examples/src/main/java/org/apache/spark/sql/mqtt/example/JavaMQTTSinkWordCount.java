package org.apache.spark.sql.mqtt.example;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.mqtt.provider.MQTTStreamSinkProvider;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Counts words in UTF-8 encoded, '\n' delimited text received from local socket
 * and publishes results on MQTT topic.
 *
 * Usage: JavaMQTTSinkWordCount <port> <brokerUrl> <topic>
 * <port> represents local network port on which program is listening for input.
 * <brokerUrl> and <topic> describe the MQTT server that structured streaming
 * would connect and send data.
 *
 * To run example on your local machine, a MQTT Server should be up and running.
 * Linux users may leverage 'nc -lk <port>' to listen on local port and wait
 * for Spark socket connection.
 */
public class JavaMQTTSinkWordCount {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaMQTTSinkWordCount <port> <brokerUrl> <topic>");
            System.exit(1);
        }

        String checkpointDir = System.getProperty("java.io.tmpdir") + "/mqtt-example/";
        // Remove checkpoint directory.
        FileUtils.deleteDirectory(new File(checkpointDir));

        Integer port = Integer.valueOf(args[0]);
        String brokerUrl = args[1];
        String topic = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("JavaMQTTSinkWordCount").master("local[4]")
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from local network socket.
        Dataset<String> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost").option("port", port)
                .load().select("value").as(Encoders.STRING());

        // Split the lines into words.
        Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        }, Encoders.STRING());

        // Generate running word count.
        Dataset<Row> wordCounts = words.groupBy("value").count();

        // Start publishing the counts to MQTT server.
        StreamingQuery query = wordCounts.writeStream()
                .format(MQTTStreamSinkProvider.class.getName())
                .option("checkpointLocation", checkpointDir)
                .outputMode("complete")
                .option("topic", topic)
                .option("localStorage", checkpointDir)
                .start(brokerUrl);

        query.awaitTermination();
    }
}
