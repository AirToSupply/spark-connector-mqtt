package org.apache.spark.sql.mqtt.write

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.mqtt.cache.CachedMQTTClient
import org.apache.spark.sql.mqtt.config.MQTTConfiguration
import org.apache.spark.sql.mqtt.util.Retry
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException}

class MQTTWriterTask(
    parameters: Map[String, String],
    schema: Seq[Attribute],
    topic: String,
    qos: Int) {

  private lazy val publishAttempts: Int = SparkEnv.get.conf.getInt(
    MQTTConfiguration.MQTT_PUBLISH_ATTEMPTS,
    MQTTConfiguration.MQTT_PUBLISH_ATTEMPTS_DEFALUT_VAL)

  private lazy val publishBackoff: Long =
    SparkEnv.get.conf.getTimeAsMs(MQTTConfiguration.MQTT_PUBLISH_BACKOFF,
      MQTTConfiguration.MQTT_PUBLISH_BACKOFF_DEFALUT_VAL)

  private var client: Option[MqttClient] = None

  def execute(iterator: Iterator[InternalRow]): Unit = {
    client = Some(CachedMQTTClient.getOrCreate(parameters))
    while (iterator.hasNext) {
      val currentRow = iterator.next()
      publish(currentRow, client.get)
    }
  }

  def publish(record: InternalRow, client: MqttClient): Unit = {
    val message = record.getBinary(0)
    Retry(publishAttempts, publishBackoff, classOf[MqttException]) {
      // In case of errors, retry sending the message.
      client.publish(topic, message, qos, false)
    }
  }

  def close(): Unit = {
    try {
      client match {
        case Some(c) =>
          c.disconnect()
          c.close()
        case _ =>
      }
    } finally {
      client = None
    }
  }

}
