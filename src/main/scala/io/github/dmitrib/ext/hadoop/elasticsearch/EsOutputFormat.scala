package io.github.dmitrib.ext.hadoop.elasticsearch

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.Text
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import scala.collection.mutable.ListBuffer
import org.slf4j.LoggerFactory
import org.apache.hadoop.conf.Configuration

object EsOutputFormat {
  val ES_CLUSTER_ENDPOINTS = "es.output.cluster.endpoints"
  val ES_CLUSTER_SNIFF = "es.output.cluster.sniff"
  val ES_BATCH_SIZE = "es.output.batch.size"
  val ES_INDEX = "es.output.index"
  val ES_TYPE = "es.output.type"

  def newClient(config: Configuration) = {
    val settings = ImmutableSettings.settingsBuilder
      .put("client.transport.ignore_cluster_name", true)
      .put("client.transport.sniff", config.getBoolean(ES_CLUSTER_SNIFF, true))
      .build

    val endpoints = Option(config.getStrings(ES_CLUSTER_ENDPOINTS)).getOrElse(Array.empty[String])
      .map(_.split(":"))
      .collect {
      case Array(host, port) => (host, port.toInt)
      case Array(host)       => (host, 9300)
    }

    endpoints.foldLeft(new TransportClient(settings)) { (cl, hostPort) =>
      val (host, port) = hostPort
      cl.addTransportAddress(new InetSocketTransportAddress(host, port))
    }
  }
}

class EsOutputFormat extends OutputFormat[Text, Text] {
  import io.github.dmitrib.ext.hadoop.elasticsearch.EsOutputFormat._

  protected val log = LoggerFactory.getLogger(this.getClass)

  def getRecordWriter(context: TaskAttemptContext): RecordWriter[Text, Text] = {
    new RecordWriter[Text, Text] {
      val client = newClient(context.getConfiguration)

      log.info(s"listed cluster nodes: ${client.listedNodes()}")
      log.info(s"connected cluster nodes: ${client.connectedNodes()}")

      val maxRequestsInBulk = context.getConfiguration.getInt(ES_BATCH_SIZE, 500)

      val index = context.getConfiguration.get(ES_INDEX)
      val itemType = context.getConfiguration.get(ES_TYPE)

      val requestQueue = ListBuffer.empty[(Option[String], String)]

      def write(key: Text, value: Text) {
        requestQueue += (Option(key).map(_.toString) -> value.toString)
        if (requestQueue.size >= maxRequestsInBulk) {
          flush()
        }
      }

      private def flush() {
        val bulk = client.prepareBulk

        for ((key, value) <- requestQueue) {
          val indexRB = client.prepareIndex(index, itemType, key.orNull).setSource(value.getBytes("UTF8"))
          bulk.add(indexRB)
        }

        val failedItems = bulk.execute().actionGet().getItems.filter(_.isFailed)
        if (!failedItems.isEmpty) {
          context.getCounter("ES", "failed index items").increment(failedItems.length)
          for (item <- failedItems) {
            log.info(s"put failed: '${item.getFailureMessage}'")
          }
        }

        requestQueue.clear()
      }

      def close(context: TaskAttemptContext) {
        if (requestQueue.nonEmpty) {
          flush()
        }
        client.close()
      }
    }
  }

  def checkOutputSpecs(context: JobContext) {}

  def getOutputCommitter(context: TaskAttemptContext) = new OutputCommitter {
    def setupJob(jobContext: JobContext) {}

    def needsTaskCommit(taskContext: TaskAttemptContext) = false

    def setupTask(taskContext: TaskAttemptContext) {}

    def commitTask(taskContext: TaskAttemptContext) {}

    def abortTask(taskContext: TaskAttemptContext) {}
  }
}
