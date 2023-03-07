package tech.odes.sql.streaming.mqtt

import java.io.File
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence
import org.scalatest.BeforeAndAfter
import org.apache.spark.SparkFunSuite
import tech.odes.utils.FileHelper

class LocalMessageStoreSuite extends SparkFunSuite with BeforeAndAfter {

  private val testData = Seq(1, 2, 3, 4, 5, 6)
  private val javaSerializer: JavaSerializer = new JavaSerializer()

  private val serializerInstance = javaSerializer
  private val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/mqtt-test2/")
  private val persistence: MqttDefaultFilePersistence =
    new MqttDefaultFilePersistence(tempDir.getAbsolutePath)

  private val store = new LocalMessageStore(persistence, javaSerializer)

  before {
    tempDir.mkdirs()
    tempDir.deleteOnExit()
    persistence.open("temp", "tcp://dummy-url:0000")
  }

  after {
    persistence.clear()
    persistence.close()
    FileHelper.deleteFileQuietly(tempDir)
  }

  test("serialize and deserialize") {
      val serialized = serializerInstance.serialize(testData)
    val deserialized: Seq[Int] = serializerInstance
      .deserialize(serialized).asInstanceOf[Seq[Int]]
    assert(testData === deserialized)
  }

  test("Store and retrieve") {
    store.store(1, testData)
    val result: Seq[Int] = store.retrieve(1)
    assert(testData === result)
  }

  test("Max offset stored") {
    store.store(1, testData)
    store.store(10, testData)
    val offset = store.maxProcessedOffset
    assert(offset == 10)
  }

}
