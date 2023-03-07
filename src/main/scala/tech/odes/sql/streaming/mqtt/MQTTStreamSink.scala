package tech.odes.sql.streaming.mqtt

import org.apache.spark.SparkEnv
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, StreamWriteSupport}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.eclipse.paho.client.mqttv3.MqttException
import tech.odes.utils.{Logging, Retry}

import scala.collection.JavaConverters._
import scala.collection.mutable

class MQTTStreamWriter (schema: StructType, parameters: DataSourceOptions)
    extends StreamWriter with Logging {
  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    // Skipping client identifier as single batch can be distributed to multiple
    // Spark worker process. MQTT server does not support two connections
    // declaring same client ID at given point in time.
    val params = parameters.asMap().asScala.filterNot(
      _._1.equalsIgnoreCase("clientId")
    )
    MQTTDataWriterFactory(params)
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

case class MQTTDataWriterFactory(config: mutable.Map[String, String])
    extends DataWriterFactory[InternalRow] {
  override def createDataWriter(
    partitionId: Int, taskId: Long, epochId: Long
  ): DataWriter[InternalRow] = new MQTTDataWriter(config)
}

case object MQTTWriterCommitMessage extends WriterCommitMessage

class MQTTDataWriter(config: mutable.Map[String, String]) extends DataWriter[InternalRow] {
  private lazy val publishAttempts: Int =
    SparkEnv.get.conf.getInt("spark.mqtt.client.publish.attempts", -1)
  private lazy val publishBackoff: Long =
    SparkEnv.get.conf.getTimeAsMs("spark.mqtt.client.publish.backoff", "5s")

  private lazy val (_, _, topic, _, _, qos, _, _, _) = MQTTUtils.parseConfigParams(config.toMap)

  override def write(record: InternalRow): Unit = {
    val client = CachedMQTTClient.getOrCreate(config.toMap)
    val message = record.getBinary(0)
    Retry(publishAttempts, publishBackoff, classOf[MqttException]) {
      // In case of errors, retry sending the message.
      client.publish(topic, message, qos, false)
    }
  }

  override def commit(): WriterCommitMessage = MQTTWriterCommitMessage

  override def abort(): Unit = {}
}

case class MQTTRelation(override val sqlContext: SQLContext, data: DataFrame)
    extends BaseRelation {
  override def schema: StructType = data.schema
}

class MQTTStreamSinkProvider extends DataSourceV2 with StreamWriteSupport
    with DataSourceRegister with CreatableRelationProvider {
  override def createStreamWriter(queryId: String, schema: StructType,
      mode: OutputMode, options: DataSourceOptions): StreamWriter = {
    new MQTTStreamWriter(schema, options)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      parameters: Map[String, String], data: DataFrame): BaseRelation = {
    MQTTRelation(sqlContext, data)
  }

  override def shortName(): String = "mqtt"
}
