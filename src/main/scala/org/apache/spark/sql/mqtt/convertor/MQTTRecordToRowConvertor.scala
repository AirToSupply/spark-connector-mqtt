package org.apache.spark.sql.mqtt.convertor

import org.apache.spark.sql.types._

import java.text.SimpleDateFormat

object MQTTRecordToRowConvertor {

  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def schema = StructType(
    StructField("id", IntegerType) ::
    StructField("topic", StringType) ::
    StructField("payload", BinaryType) ::
    StructField("timestamp", TimestampType) ::
    Nil)
}
