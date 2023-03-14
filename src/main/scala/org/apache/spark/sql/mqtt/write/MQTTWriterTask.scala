package org.apache.spark.sql.mqtt.write

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.mqtt.cache.CachedMQTTClient
import org.apache.spark.sql.mqtt.config.MQTTConfiguration
import org.apache.spark.sql.mqtt.util.Retry
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttException}

class MQTTWriterTask(
    partitionId: Int,
    parameters: Map[String, String],
    schema: Seq[Attribute],
    topic: String,
    qos: Int) extends Logging {

  private lazy val publishAttempts: Int = SparkEnv.get.conf.getInt(
    MQTTConfiguration.MQTT_PUBLISH_ATTEMPTS,
    MQTTConfiguration.MQTT_PUBLISH_ATTEMPTS_DEFALUT_VAL)

  private lazy val publishBackoff: Long =
    SparkEnv.get.conf.getTimeAsMs(MQTTConfiguration.MQTT_PUBLISH_BACKOFF,
      MQTTConfiguration.MQTT_PUBLISH_BACKOFF_DEFALUT_VAL)

  def execute(iterator: Iterator[InternalRow]): Unit = {
    logInfo(
      s"""
         |---------------------------------------------
         |MQTTWriterTask -> Publish (Partition: ${partitionId})
         |--------------------------------------------
         |""".stripMargin)

    val client: Option[MqttClient] = Some(CachedMQTTClient.getOrCreate(parameters))
    while (iterator.hasNext) {
      val record = iterator.next()
      Retry(publishAttempts, publishBackoff, classOf[MqttException]) {
        // In case of errors, retry sending the message.
        client.get.publish(topic, record.getBinary(0), qos, false)
      }
    }
  }
}
