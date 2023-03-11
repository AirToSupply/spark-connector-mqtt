package org.apache.spark.sql.mqtt.write

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.util.Utils

object MQTTWriter extends Logging {

  def validateQuery(schema: Seq[Attribute],
                    parameters: Map[String, String]): Unit = {
    // TO DO
  }

  def write(
    sparkSession: SparkSession,
    queryExecution: QueryExecution,
    parameters: Map[String, String],
    topic: String,
    qos: Int): Unit = {
    val schema = queryExecution.analyzed.output
    validateQuery(schema, parameters)
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask = new MQTTWriterTask(parameters, schema, topic, qos)
      Utils.tryWithSafeFinally(writeTask.execute(iter))(writeTask.close())
    }
  }
}
