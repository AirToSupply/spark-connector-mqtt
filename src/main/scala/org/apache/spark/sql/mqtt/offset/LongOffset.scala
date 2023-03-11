package org.apache.spark.sql.mqtt.offset

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset, Offset => OffsetV2}

/**
 * A simple offset for sources that produce a single linear stream of data.
 */
case class LongOffset(offset: Long) extends OffsetV2 {

  override val json = offset.toString

  def +(increment: Long): LongOffset = new LongOffset(offset + increment)
  def -(decrement: Long): LongOffset = new LongOffset(offset - decrement)
}

object LongOffset {

  /**
   * LongOffset factory from serialized offset.
   *
   * @return new LongOffset
   */
  def apply(offset: SerializedOffset) : LongOffset = new LongOffset(offset.json.toLong)

  /**
   * Convert generic Offset to LongOffset if possible.
   *
   * @return converted LongOffset
   */
  def convert(offset: Offset): Option[LongOffset] = offset match {
    case lo: LongOffset => Some(lo)
    case so: SerializedOffset => Some(LongOffset(so))
    case _ => None
  }
}
