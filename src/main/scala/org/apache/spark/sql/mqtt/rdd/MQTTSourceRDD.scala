package org.apache.spark.sql.mqtt.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.mqtt.store.{MQTTMessage, MessageStore}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.concurrent.TrieMap

case class MQTTSourceRDDPartition(
  index: Int, range: MQTTOffsetRange) extends Partition {
  override def toString: String =
    s"""
      |MQTTSourceRDDPartition
      |  index         : ${index}
      |  partitionIndex: ${range.partitionIndex}
      |  start         : ${range.startOffset}
      |  end           : ${range.endOffset}
      |""".stripMargin
}

case class MQTTOffsetRange(
  partitionIndex: Int,
  startOffset: Long,
  endOffset: Long,
  preferredLoc: Option[String] = None) {
  def size: Long = endOffset - startOffset
  def partition: Int = partitionIndex
}

class MQTTSourceRDD(
  sc: SparkContext,
  offsetRanges: Seq[MQTTOffsetRange],
  messages: TrieMap[Long, MQTTMessage],
  // The function is used here to avoid that the storage handle cannot be serialized
  store: (Long, Long) => Map[Long, MQTTMessage])
  extends RDD[MQTTMessage](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = super.persist(newLevel)

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
      new MQTTSourceRDDPartition(i, o)
    }.toArray
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val part = partition.asInstanceOf[MQTTSourceRDDPartition]
    part.range.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(
      partition: Partition,
      context: TaskContext): Iterator[MQTTMessage] = {
    val part = partition.asInstanceOf[MQTTSourceRDDPartition]
    logInfo(
      s"""
         |------------------------------------------
         | Compute:
         |------------------------------------------
         | ${part}
         |------------------------------------------
         |""".stripMargin)

    part.range.size match {
      case 0 =>
        Iterator.empty
      case _ =>
        new Iterator[MQTTMessage] {
          private var cursor = part.range.startOffset
          private def index = cursor - 1
          private def _store = store(part.range.startOffset, part.range.endOffset)
          override def hasNext: Boolean = cursor < part.range.endOffset
          override def next(): MQTTMessage = {
            cursor += 1
            val _idx = index
            messages.getOrElse[MQTTMessage](_idx, _store.getOrElse(_idx, {
              val error = s"""
                   |------------------------------------------
                   | Compute Error: Message Index[${_idx}] is not found for this partition
                   |------------------------------------------
                   |${part}
                   |------------------------------------------
                   |""".stripMargin
              logInfo(error)
              throw new RuntimeException(error)
            }))
          }
        }
    }
  }
}
