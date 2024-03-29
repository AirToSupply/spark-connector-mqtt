package org.apache.spark.sql.mqtt.config

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.eclipse.paho.client.mqttv3.persist.{MemoryPersistence, MqttDefaultFilePersistence}
import org.eclipse.paho.client.mqttv3.{MqttClient, MqttClientPersistence, MqttConnectOptions}

import java.util.Properties
object MQTTConfiguration extends Logging {

  // Since data source configuration properties are case-insensitive,
  // we have to introduce our own keys. Also, good for vendor independence.
  private val sslParamMapping = Map(
    "ssl.protocol" -> "com.ibm.ssl.protocol",
    "ssl.key.store" -> "com.ibm.ssl.keyStore",
    "ssl.key.store.password" -> "com.ibm.ssl.keyStorePassword",
    "ssl.key.store.type" -> "com.ibm.ssl.keyStoreType",
    "ssl.key.store.provider" -> "com.ibm.ssl.keyStoreProvider",
    "ssl.trust.store" -> "com.ibm.ssl.trustStore",
    "ssl.trust.store.password" -> "com.ibm.ssl.trustStorePassword",
    "ssl.trust.store.type" -> "com.ibm.ssl.trustStoreType",
    "ssl.trust.store.provider" -> "com.ibm.ssl.trustStoreProvider",
    "ssl.ciphers" -> "com.ibm.ssl.enabledCipherSuites",
    "ssl.key.manager" -> "com.ibm.ssl.keyManager",
    "ssl.trust.manager" -> "com.ibm.ssl.trustManager"
  )

  // publish attempts
  val MQTT_PUBLISH_ATTEMPTS = "spark.mqtt.client.publish.attempts"
  val MQTT_PUBLISH_ATTEMPTS_DEFALUT_VAL = -1

  // publish backoff
  val MQTT_PUBLISH_BACKOFF = "spark.mqtt.client.publish.backoff"
  val MQTT_PUBLISH_BACKOFF_DEFALUT_VAL = "5s"

  def parseConfigParams(config: Map[String, String]):
    (String, String, String, MqttClientPersistence, MqttConnectOptions, Int, Long, Long, Int) = {

    val parameters = CaseInsensitiveMap(config)

    val brokerUrl: String = parameters.getOrElse("brokerUrl",
      parameters.getOrElse("path",
        throw new IllegalArgumentException(
          "Please provide a `brokerUrl` by specifying path or .options(\"brokerUrl\",...)")))

    val persistence: MqttClientPersistence = parameters.get("persistence") match {
      case Some("memory") => new MemoryPersistence()
      case _ => parameters.get("localStorage") match {
          case Some(x) => new MqttDefaultFilePersistence(x)
          case None => new MqttDefaultFilePersistence()
        }
    }

    // if default is subscribe everything, it leads to getting lot unwanted system messages.
    val topic: String = parameters.getOrElse("topic",
      throw new IllegalArgumentException("Please specify a topic, by .options(\"topic\",...)"))

    val clientId: String = parameters.getOrElse("clientId", {
      log.warn("If `clientId` is not set, a random value is picked up." +
        "\nRecovering from failure is not supported in such a case.")
      MqttClient.generateClientId()})

    val username: Option[String] = parameters.get("username")
    val password: Option[String] = parameters.get("password")
    val connectionTimeout: Int = parameters.getOrElse("connectionTimeout",
      MqttConnectOptions.CONNECTION_TIMEOUT_DEFAULT.toString).toInt
    val keepAlive: Int = parameters.getOrElse("keepAlive", MqttConnectOptions
      .KEEP_ALIVE_INTERVAL_DEFAULT.toString).toInt
    val mqttVersion: Int = parameters.getOrElse("mqttVersion", MqttConnectOptions
      .MQTT_VERSION_DEFAULT.toString).toInt
    val cleanSession: Boolean = parameters.getOrElse("cleanSession", "false").toBoolean
    val qos: Int = parameters.getOrElse("QoS", "1").toInt
    val autoReconnect: Boolean = parameters.getOrElse("autoReconnect", "false").toBoolean
    val maxInflight: Int = parameters.getOrElse("maxInflight", "60").toInt

    val maxBatchMessageNum = parameters.getOrElse("maxBatchMessageNum", s"${Long.MaxValue}").toLong
    val maxBatchMessageSize = parameters.getOrElse("maxBatchMessageSize",
      s"${Long.MaxValue}").toLong
    val maxRetryNumber = parameters.getOrElse("maxRetryNum", "3").toInt

    val mqttConnectOptions: MqttConnectOptions = new MqttConnectOptions()
    mqttConnectOptions.setAutomaticReconnect(autoReconnect)
    mqttConnectOptions.setCleanSession(cleanSession)
    mqttConnectOptions.setConnectionTimeout(connectionTimeout)
    mqttConnectOptions.setKeepAliveInterval(keepAlive)
    mqttConnectOptions.setMqttVersion(mqttVersion)
    mqttConnectOptions.setMaxInflight(maxInflight)

    (username, password) match {
      case (Some(u: String), Some(p: String)) =>
        mqttConnectOptions.setUserName(u)
        mqttConnectOptions.setPassword(p.toCharArray)
      case _ =>
    }

    val sslProperties = new Properties()
    config.foreach(e => {
      if (e._1.startsWith("ssl.")) {
        sslProperties.setProperty(sslParamMapping(e._1), e._2)
      }
    })
    mqttConnectOptions.setSSLProperties(sslProperties)

    (
      brokerUrl,
      clientId,
      topic,
      persistence,
      mqttConnectOptions,
      qos,
      maxBatchMessageNum,
      maxBatchMessageSize,
      maxRetryNumber)
  }
}
