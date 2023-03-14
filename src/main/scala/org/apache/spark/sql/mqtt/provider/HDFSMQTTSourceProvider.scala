package org.apache.spark.sql.mqtt.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.mqtt.config.MQTTConfiguration
import org.apache.spark.sql.mqtt.convertor.MQTTRecordToRowConvertor
import org.apache.spark.sql.mqtt.source.HDFSBasedMQTTStreamSource
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

/**
 * The provider class for creating MQTT source.
 * This provider throw IllegalArgumentException if  'brokerUrl' or 'topic' parameter
 * is not set in options.
 */
class HDFSMQTTSourceProvider extends StreamSourceProvider
  with DataSourceRegister
  with Logging {

  override def sourceSchema(sqlContext: SQLContext, schema: Option[StructType],
    providerName: String, parameters: Map[String, String]): (String, StructType) = {
    ("hdfs-mqtt", MQTTRecordToRowConvertor.schema)
  }

  override def createSource(sqlContext: SQLContext, metadataPath: String,
    schema: Option[StructType], providerName: String, parameters: Map[String, String]): Source = {

    val parsedResult = MQTTConfiguration.parseConfigParams(parameters)

    new HDFSBasedMQTTStreamSource(
      sqlContext,
      metadataPath,
      parsedResult._1, // brokerUrl
      parsedResult._2, // clientId
      parsedResult._3, // topic
      parsedResult._5, // mqttConnectionOptions
      parsedResult._6, // qos
      parsedResult._7, // maxBatchMessageNum
      parsedResult._8, // maxBatchMessageSize
      parsedResult._9  // maxRetryNum
    )
  }

  override def shortName(): String = "hdfs-mqtt"
}

object HDFSMQTTSourceProvider {
  val SEP = "##"
}
