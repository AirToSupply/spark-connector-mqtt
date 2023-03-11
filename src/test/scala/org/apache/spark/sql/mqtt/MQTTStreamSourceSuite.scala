package org.apache.spark.sql.mqtt

import org.apache.spark.sql._
import org.apache.spark.sql.mqtt.util.FileHelper
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}
import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable

class MQTTStreamSourceSuite extends SparkFunSuite
  with Eventually with SharedSparkContext with BeforeAndAfter {

  protected var mqttTestUtils: MQTTTestUtils = _
  protected val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")

  before {
    tempDir.mkdirs()
    if (!tempDir.exists()) {
      throw new IllegalStateException("Unable to create temp directories.")
    }
    tempDir.deleteOnExit()
    mqttTestUtils = new MQTTTestUtils(tempDir)
    mqttTestUtils.setup()
  }

  after {
    mqttTestUtils.teardown()
    FileHelper.deleteFileQuietly(tempDir)
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def writeStreamResults(sqlContext: SQLContext, dataFrame: DataFrame): StreamingQuery = {
    import sqlContext.implicits._
    val query: StreamingQuery = dataFrame.selectExpr("CAST(payload AS STRING)").as[String]
      .writeStream.format("parquet").start(s"$tmpDir/t.parquet")
    while (!query.status.isTriggerActive) {
      Thread.sleep(20)
    }
    query
  }

  protected def readBackStreamingResults(sqlContext: SQLContext): mutable.Buffer[String] = {
    import sqlContext.implicits._
    val asList =
      sqlContext.read
        .parquet(s"$tmpDir/t.parquet").as[String]
        .collectAsList().asScala
    asList
  }

  protected def createStreamingDataFrame(dir: String = tmpDir,
                                         filePersistence: Boolean = false): (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = SparkSession.builder()
      .getOrCreate().sqlContext

    sqlContext.setConf("spark.sql.streaming.checkpointLocation", tmpDir)

    val ds: DataStreamReader =
      sqlContext.readStream.format("tech.odes.sql.streaming.mqtt.MQTTStreamSourceProvider")
        .option("topic", "test").option("clientId", "clientId").option("connectionTimeout", "120")
        .option("keepAlive", "1200").option("maxInflight", "120").option("autoReconnect", "false")
        .option("cleanSession", "true").option("QoS", "2")

    val dataFrame = if (!filePersistence) {
      ds.option("persistence", "memory").load("tcp://" + mqttTestUtils.brokerUri)
    } else {
      ds.option("persistence", "file").option("localStorage", tmpDir)
        .load("tcp://" + mqttTestUtils.brokerUri)
    }
    (sqlContext, dataFrame)
  }

}

class BasicMQTTSourceSuite extends MQTTStreamSourceSuite {

  test("basic usage") {

    val sendMessage = "MQTT is a message queue."

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataFrame()

    val query = writeStreamResults(sqlContext, dataFrame)
    mqttTestUtils.publishData("test", sendMessage)
    query.processAllAvailable()
    query.awaitTermination(10000)

    val resultBuffer: mutable.Buffer[String] = readBackStreamingResults(sqlContext)

    assert(resultBuffer.size == 1)
    assert(resultBuffer.head == sendMessage)
  }

  test("Send and receive 50 messages.") {

    val sendMessage = "MQTT is a message queue."

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataFrame()

    val q = writeStreamResults(sqlContext, dataFrame)

    mqttTestUtils.publishData("test", sendMessage, 50)
    q.processAllAvailable()
    q.awaitTermination(10000)

    val resultBuffer: mutable.Buffer[String] = readBackStreamingResults(sqlContext)

    assert(resultBuffer.size == 50)
    assert(resultBuffer.head == sendMessage)
  }

}

class StressTestMQTTSource extends MQTTStreamSourceSuite {

  // Run with -Xmx1024m
  test("Send and receive messages of size 100MB.") {

    val freeMemory: Long = Runtime.getRuntime.freeMemory()

    log.info(s"Available memory before test run is ${freeMemory / (1024 * 1024)}MB.")

    val noOfMsgs: Int = (100 * 1024 * 1024) / (500 * 1024) // 204

    val messageBuilder = new StringBuilder()
    for (i <- 0 until (500 * 1024)) yield messageBuilder.append(((i % 26) + 65).toChar)
    val sendMessage = messageBuilder.toString() // each message is 50 KB

    val (sqlContext: SQLContext, dataFrame: DataFrame) = createStreamingDataFrame()

    val query = writeStreamResults(sqlContext, dataFrame)
    mqttTestUtils.publishData("test", sendMessage, noOfMsgs )
    query.processAllAvailable()
    query.awaitTermination(25000)

    val messageCount =
      sqlContext.read
        .parquet(s"$tmpDir/t.parquet")
        .count()
    assert(messageCount == noOfMsgs)
  }
}