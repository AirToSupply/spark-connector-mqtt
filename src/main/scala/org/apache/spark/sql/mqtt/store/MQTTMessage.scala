package org.apache.spark.sql.mqtt.store

import org.apache.spark.sql.mqtt.convertor.MQTTRecordToRowConvertor
import org.eclipse.paho.client.mqttv3.MqttMessage

import java.nio.charset.Charset
import java.sql.Timestamp
import java.util.Calendar

class MQTTMessage(m: MqttMessage, val topic: String) extends Serializable {

  // TODO: make it configurable.
  val timestamp: Timestamp = Timestamp.valueOf(
    MQTTRecordToRowConvertor.DATE_FORMAT.format(Calendar.getInstance().getTime))

  val duplicate = m.isDuplicate

  val retained = m.isRetained

  val qos = m.getQos

  val id: Int = m.getId

  val payload: Array[Byte] = m.getPayload

  override def toString(): String = {
    s"""MQTTMessage.
       |Topic: ${this.topic}
       |MessageID: ${this.id}
       |QoS: ${this.qos}
       |Payload: ${this.payload}
       |Payload as string: ${new String(this.payload, Charset.defaultCharset())}
       |isRetained: ${this.retained}
       |isDuplicate: ${this.duplicate}
       |TimeStamp: ${this.timestamp}
     """.stripMargin
  }
}
