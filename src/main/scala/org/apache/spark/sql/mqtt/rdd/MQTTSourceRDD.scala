package org.apache.spark.sql.mqtt.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.mqtt.store.{MQTTMessage, MessageStore}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.concurrent.TrieMap

case class MQTTSourceRDDPartition(
  index: Int, offsetRange: MQTTOffsetRange) extends Partition

case class MQTTOffsetRange(
  partitionIndex: Int,
  startOffset: Long,
  endOffset: Long,
  preferredLoc: Option[String] = None) {
  def size: Long = endOffset - startOffset + 1
  def partition: Int = partitionIndex
}

class MQTTSourceRDD(
  sc: SparkContext,
  offsetRanges: Seq[MQTTOffsetRange],
  messages: TrieMap[Long, MQTTMessage],
  store: MessageStore)
  extends RDD[MQTTMessage](sc, Nil) {

  override def persist(newLevel: StorageLevel): this.type = super.persist(newLevel)

  override def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
      new MQTTSourceRDDPartition(i, o)
    }.toArray
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val part = partition.asInstanceOf[MQTTSourceRDDPartition]
    part.offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(
       partition: Partition,
      context: TaskContext): Iterator[MQTTMessage] = {
    val part = partition.asInstanceOf[MQTTSourceRDDPartition]
    if (part.offsetRange.size == 0) {
      Iterator.empty
    } else {
      val itr = new Iterator[MQTTMessage] {

        private var currentIdx = -1

        override def hasNext: Boolean = {
          currentIdx += 1
          currentIdx < part.offsetRange.size
        }

        override def next(): MQTTMessage = {
          messages.getOrElse(currentIdx, store.retrieve[MQTTMessage](currentIdx))
        }
      }
      itr
    }
  }
}
