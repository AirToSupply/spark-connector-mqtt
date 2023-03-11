package org.apache.spark.sql.mqtt.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.mqtt.config.MQTTConfiguration
import org.apache.spark.sql.mqtt.sink.MQTTStreamSink
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class MQTTStreamSinkProvider extends DataSourceRegister
  with StreamSinkProvider
  with CreatableRelationProvider
  with Logging {

  override def createSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode): Sink = {
    /**
     * Skipping client identifier as single batch can be distributed to multiple Spark worker process.
     * MQTT server does not support two connections declaring same client ID at given point in time.
     */
    val params = parameters.filterNot(_._1.equalsIgnoreCase("clientId"))

    val (
      brokerUrl,
      _,
      topic,
      persistence,
      mqttConnectOptions,
      qos, _, _, _) = MQTTConfiguration.parseConfigParams(params)

    new MQTTStreamSink(
      sqlContext,
      params,
      partitionColumns,
      outputMode,
      brokerUrl,
      persistence,
      topic,
      mqttConnectOptions,
      qos)
  }

  override def shortName(): String = "mqtt"

  override def createRelation(
     sqlContext: SQLContext,
     mode: SaveMode,
     parameters: Map[String, String],
     data: DataFrame): BaseRelation = new BaseRelation {

    override def sqlContext: SQLContext = sqlContext

    override def schema: StructType = data.schema
  }
}
