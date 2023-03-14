package org.apache.spark.sql.mqtt.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.mqtt.config.MQTTConfiguration
import org.apache.spark.sql.mqtt.convertor.MQTTRecordToRowConvertor
import org.apache.spark.sql.mqtt.source.MQTTStreamSource
import org.apache.spark.sql.mqtt.store.LocalMessageStore
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class MQTTStreamSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with Logging {

  override def shortName(): String = "mqtt"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) =
    (providerName, MQTTRecordToRowConvertor.schema)

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val (
      brokerUrl,
      clientId,
      topic,
      persistence,
      mqttConnectOptions,
      qos, _, _, _) = MQTTConfiguration.parseConfigParams(parameters)

    new MQTTStreamSource(
      sqlContext,
      parameters,
      brokerUrl,
      persistence,
      topic,
      clientId,
      mqttConnectOptions,
      qos)
  }
}
