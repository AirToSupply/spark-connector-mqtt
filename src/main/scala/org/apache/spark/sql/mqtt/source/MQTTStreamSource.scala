package org.apache.spark.sql.mqtt.source

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source, Offset => OffsetV2}
import org.apache.spark.sql.mqtt.convertor.MQTTRecordToRowConvertor
import org.apache.spark.sql.mqtt.offset.LongOffset
import org.apache.spark.sql.mqtt.rdd.{MQTTOffsetRange, MQTTSourceRDD}
import org.apache.spark.sql.mqtt.store.{LocalMessageStore, MQTTMessage}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.unsafe.types.UTF8String
import org.eclipse.paho.client.mqttv3._

import javax.annotation.concurrent.GuardedBy
import scala.collection.concurrent.TrieMap

/**
 * A mqtt stream source.
 *
 * @param brokerUrl
 *          url MqttClient connects to.
 *
 * @param persistence
 *          an instance of MqttClientPersistence. By default it is used for storing
 *          incoming messages on disk. If memory is provided as option, then recovery
 *          on restart is not supported.
 *
 * @param topic
 *           topic MqttClient subscribes to.
 *
 * @param clientId
 *           clientId, this client is associated with.
 *           Provide the same value to recover a stopped client.
 *
 * @param mqttConnectOptions
 *           an instance of MqttConnectOptions for this Source.
 *
 * @param qos
 *           the maximum quality of service to subscribe each topic at.
 *           Messages published at a lower quality of service will be received at the published QoS.
 *           Messages published at a higher quality of service will be received using
 *           the QoS specified on the subscribe.
 */
class MQTTStreamSource(
  sqlContext: SQLContext,
  options: Map[String, String],
  brokerUrl: String,
  persistence: MqttClientPersistence,
  topic: String,
  clientId: String,
  mqttConnectOptions: MqttConnectOptions,
  qos: Int) extends Source with Logging {

  override def schema: StructType = MQTTRecordToRowConvertor.schema

  private var startOffset: OffsetV2 = _

  private var endOffset: OffsetV2 = _

  /**
   * Older than last N messages, will not be checked for redelivery.
   */
  val backLog = options.getOrElse("autopruning.backlog", "500").toInt

  private val store = new LocalMessageStore(persistence)

  private val messages = new TrieMap[Long, MQTTMessage]

  @GuardedBy("this")
  private var currentOffset: LongOffset = LongOffset(-1L)

  @GuardedBy("this")
  private var lastOffsetCommitted: LongOffset = LongOffset(-1L)

  private var client: MqttClient = _

  private def getCurrentOffset = currentOffset

  initialize

  /**
   * Initial MqttClient and add mqtt callback function
   */
  private def initialize(): Unit = {

    client = new MqttClient(brokerUrl, clientId, persistence)

    val callback = new MqttCallbackExtended() {

      override def messageArrived(topic_ : String, message: MqttMessage): Unit = synchronized {
        val mqttMessage = new MQTTMessage(message, topic_)
        val offset = currentOffset.offset + 1L
        messages.put(offset, mqttMessage)
        store.store(offset, mqttMessage)
        currentOffset = LongOffset(offset)
        log.trace(s"Message arrived, $topic_ $mqttMessage")
      }

      override def deliveryComplete(token: IMqttDeliveryToken): Unit = {
        // callback for publisher, no need here.
      }

      override def connectionLost(cause: Throwable): Unit = {
        log.warn("Connection to mqtt server lost.", cause)
      }

      override def connectComplete(reconnect: Boolean, serverURI: String): Unit = {
        log.info(s"Connect complete $serverURI. Is it a reconnect?: $reconnect")
      }
    }

    client.setCallback(callback)
    client.connect(mqttConnectOptions)
    client.subscribe(topic, qos)
  }

  private def setOffsetRange(start: Option[OffsetV2], end: Option[OffsetV2]): Unit = synchronized {
    startOffset = start.getOrElse(LongOffset(-1L))
    endOffset = end.getOrElse(currentOffset)
  }

  private def getStartOffset: OffsetV2 = Option(startOffset).getOrElse(
    throw new IllegalStateException("start offset not set"))

  private def getEndOffset: OffsetV2 = Option(endOffset).getOrElse(
    throw new IllegalStateException("end offset not set"))

  /**
   * Monitor the change of MQTT messages and obtain the offset
   *
   * @return Option[Offset]
   */
  override def getOffset: Option[Offset] = Some(getCurrentOffset)

  private def getOffsetRangesFromResolvedOffsets(
      start: Long, end: Long, numPartitions: Int): Array[MQTTOffsetRange] = {
    if (numPartitions == 1) {
      return Array(MQTTOffsetRange(0, start, end))
    }

    val ranges = Array.ofDim[MQTTOffsetRange](numPartitions)
    val num = end - start + 1
    val (delta, mod) = num % numPartitions match {
      case 0 => (num / numPartitions, 0)
      case _@m => (num / numPartitions + 1, m)
    }
    for (idx <- 1 to numPartitions) {
      val (from, to) = mod match {
        case 0 =>
          val from = start + (idx - 1) * delta
          val to = from + (delta - 1)
          (from, to)
        case _ =>
          val from = idx match {
            case numPartitions => end - (mod - 1)
            case _ => start + (idx - 1) * delta
          }
          val to = idx match {
            case numPartitions => end
            case _ => from + (delta - 1)
          }
          (from, to)
      }
      ranges(idx - 1) = MQTTOffsetRange(idx - 1, from, to)
    }
    ranges
  }

  /**
   * Obtain data for calculation
   *
   * @param start
   *          last offset
   * @param end
   *          latest offset
   * @return
   */
  override def getBatch(start: Option[OffsetV2], end: OffsetV2): DataFrame = {
    log.info(s"GetBatch called with start = $start, end = $end")
    setOffsetRange(start, Some(end))

    val sc = sqlContext.sparkContext
    val numPartitions = sc.defaultParallelism

    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(
        sc.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }

    val sliceStart = LongOffset.convert(startOffset).get.offset + 1
    val sliceEnd = LongOffset.convert(endOffset).get.offset + 1
    val offsetRanges: Seq[MQTTOffsetRange] =
      getOffsetRangesFromResolvedOffsets(sliceStart, sliceEnd, numPartitions)

    val rdd = new MQTTSourceRDD(
      sc, offsetRanges, messages, store).map { mm =>
      InternalRow(
        mm.id,
        UTF8String.fromString(mm.topic),
        mm.payload,
        mm.timestamp match {
          case null => null
          case value => value.getTime * 1000
        })
    }

    sqlContext.internalCreateDataFrame(
      rdd.setName("mqtt"), schema, isStreaming = true)
  }

  override def commit(end: OffsetV2): Unit = synchronized {

    val newOffset = LongOffset.convert(end).getOrElse(sys.error(
      s"MQTTStreamSource.commit() received an offset ($end) that did not originate with an instance of this class")
    )

    val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

    if (offsetDiff < 0) {
      sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
    }

    (lastOffsetCommitted.offset until newOffset.offset).foreach { x =>
      messages.remove(x + 1)
      store.remove(x + 1)
    }
    lastOffsetCommitted = newOffset
  }

  /**
   * Stop mqtt client connection and release persistence
   */
  override def stop(): Unit = synchronized {
    client.disconnect()
    persistence.close()
    client.close()
  }

  override def deserializeOffset(json: String): OffsetV2 = {
    LongOffset(json.toLong)
  }

  override def toString: String =
    s"""
      |MQTTStreamSource [brokerUrl: $brokerUrl, topic: $topic clientId: $clientId]
      |""".stripMargin
}
