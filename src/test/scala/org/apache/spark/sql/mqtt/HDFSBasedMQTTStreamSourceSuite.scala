package org.apache.spark.sql.mqtt

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.security.Groups
import org.apache.spark.sql._
import org.apache.spark.sql.mqtt.convertor.MQTTRecordToRowConvertor
import org.apache.spark.sql.mqtt.provider.HDFSMQTTSourceProvider
import org.apache.spark.sql.mqtt.source.HDFSBasedMQTTStreamSource
import org.apache.spark.sql.mqtt.util.FileHelper
import org.apache.spark.sql.streaming.{DataStreamReader, StreamingQuery}
import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.eclipse.paho.client.mqttv3.MqttException
import org.scalatest.BeforeAndAfter

import java.io.File
import scala.collection.JavaConverters._
import scala.collection.mutable

class HDFSBasedMQTTStreamSourceSuite
    extends SparkFunSuite
    with SharedSparkContext
    with BeforeAndAfter {

  protected var mqttTestUtils: MQTTTestUtils = _
  protected val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test/")
  protected var hadoop: MiniDFSCluster = _

  before {
    tempDir.mkdirs()
    if (!tempDir.exists()) {
      throw new IllegalStateException("Unable to create temp directories.")
    }
    tempDir.deleteOnExit()
    mqttTestUtils = new MQTTTestUtils(tempDir)
    mqttTestUtils.setup()
    hadoop = HDFSTestUtils.prepareHadoop()
  }

  after {
    mqttTestUtils.teardown()
    HDFSTestUtils.shutdownHadoop()
    FileHelper.deleteFileQuietly(tempDir)
  }

  protected val tmpDir: String = tempDir.getAbsolutePath

  protected def writeStreamResults(sqlContext: SQLContext, dataFrame: DataFrame): StreamingQuery = {
    import sqlContext.implicits._
    val query: StreamingQuery = dataFrame.selectExpr("CAST(payload AS STRING)").as[String]
      .writeStream.format("csv").start(s"$tempDir/t.csv")
    while (!query.status.isTriggerActive) {
      Thread.sleep(20)
    }
    query
  }

  protected def readBackStreamingResults(sqlContext: SQLContext): mutable.Buffer[String] = {
    import sqlContext.implicits._
    val asList =
      sqlContext.read
        .csv(s"$tmpDir/t.csv").as[String]
        .collectAsList().asScala
    asList
  }

  protected def createStreamingDataFrame(dir: String = tmpDir): (SQLContext, DataFrame) = {

    val sqlContext: SQLContext = SparkSession.builder()
      .getOrCreate().sqlContext

    sqlContext.setConf("spark.sql.streaming.checkpointLocation",
      s"hdfs://localhost:${hadoop.getNameNodePort}/testCheckpoint")

    val ds: DataStreamReader =
      sqlContext.readStream.format("org.apache.spark.sql.mqtt.provider.HDFSMQTTSourceProvider")
        .option("topic", "test").option("clientId", "clientId").option("connectionTimeout", "120")
        .option("keepAlive", "1200").option("autoReconnect", "false")
        .option("cleanSession", "true").option("QoS", "2")
    val dataFrame = ds.load("tcp://" + mqttTestUtils.brokerUri)
    (sqlContext, dataFrame)
  }
}

object HDFSTestUtils {

  private var hadoop: MiniDFSCluster = _

  def prepareHadoop(): MiniDFSCluster = {
    if (hadoop != null) {
      hadoop
    } else {
      val baseDir = new File(System.getProperty("java.io.tmpdir") + "/hadoop").getAbsoluteFile
      System.setProperty("HADOOP_USER_NAME", "test")
      val conf = new Configuration
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
      conf.setBoolean("dfs.namenode.acls.enabled", true)
      conf.setBoolean("dfs.permissions", true)
      Groups.getUserToGroupsMappingService(conf)
      val builder = new MiniDFSCluster.Builder(conf)
      hadoop = builder.build
      conf.set("fs.defaultFS", "hdfs://localhost:" + hadoop.getNameNodePort + "/")
      HDFSBasedMQTTStreamSource.hadoopConfig = conf
      hadoop
    }
  }

  def shutdownHadoop(): Unit = {
    if (null != hadoop) {
      hadoop.shutdown(true)
    }
    hadoop = null
  }
}

class BasicHDFSBasedMQTTSourceSuite extends HDFSBasedMQTTStreamSourceSuite {

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

  test("no server up") {
    val provider = new HDFSMQTTSourceProvider
    val sqlContext: SQLContext = SparkSession.builder().getOrCreate().sqlContext
    intercept[MqttException] {
      provider.createSource(
        sqlContext,
        s"hdfs://localhost:${hadoop.getNameNodePort}/testCheckpoint/0",
        Some(MQTTRecordToRowConvertor.schema),
        "org.apache.spark.sql.mqtt.provider.HDFSMQTTSourceProvider",
        Map("brokerUrl" -> "tcp://localhost:1881", "topic" -> "test")
      )
    }
  }

  test("params not provided.") {
    val provider = new HDFSMQTTSourceProvider
    val sqlContext: SQLContext = SparkSession.builder().getOrCreate().sqlContext
    intercept[IllegalArgumentException] {
      provider.createSource(
        sqlContext,
        s"hdfs://localhost:${hadoop.getNameNodePort}/testCheckpoint/0",
        Some(MQTTRecordToRowConvertor.schema),
        "org.apache.spark.sql.mqtt.provider.HDFSMQTTSourceProvider",
        Map()
      )
    }
  }
}
