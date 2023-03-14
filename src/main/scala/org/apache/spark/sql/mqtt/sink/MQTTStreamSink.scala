package org.apache.spark.sql.mqtt.sink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.mqtt.write.MQTTWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.eclipse.paho.client.mqttv3.{MqttClientPersistence, MqttConnectOptions}

class MQTTStreamSink(
  sqlContext: SQLContext,
  parameters: Map[String, String],
  partitionColumns: Seq[String],
  outputMode: OutputMode,
  brokerUrl: String,
  persistence: MqttClientPersistence,
  topic: String,
  mqttConnectOptions: MqttConnectOptions,
  qos: Int) extends Sink with Logging {

  @volatile private var latestBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      log.info(s"Skipping already committed batch $batchId")
    } else {
      MQTTWriter.write(sqlContext.sparkSession, data.queryExecution, parameters,
        topic, qos)
      latestBatchId = batchId
    }
  }

  override def toString: String =
    s"""
       |MQTTStreamSource [brokerUrl: $brokerUrl, topic: $topic]
       |""".stripMargin
}
