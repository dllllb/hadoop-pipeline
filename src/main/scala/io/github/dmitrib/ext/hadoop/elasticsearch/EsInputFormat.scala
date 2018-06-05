package io.github.dmitrib.ext.hadoop.elasticsearch

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.{Writable, Text}
import java.util
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import java.io.{DataOutput, DataInput}
import scala.collection.JavaConverters._
import org.apache.hadoop.conf.Configuration
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.slf4j.LoggerFactory

object EsInputFormat {
  val ES_CLUSTER_ENDPOINTS = "es.input.cluster.endpoints"
  val ES_CLUSTER_SNIFF = "es.input.cluster.sniff"
  val ES_BATCH_SIZE = "es.input.batch.size"
  val ES_INDEX = "es.input.index"
  val ES_TYPE = "es.input.type"
  val ES_RETRY_COUNT = "es.input.retry.count"
  val ES_TIMEOUT_MINS = "es.input.timeout.mins"
  val ES_QUERY = "es.input.query"
  val ES_HITS_PER_SHARD = "es.input.shard.hits.count"
  val ES_INCLUDE_FIELDS = "es.input.include.fields"
  val ES_EXCLUDE_FIELDS = "es.input.exclude.fields"
  val ES_QUERY_TAG = "es.query.tag"
  val ES_DOCUMENT_VERSIONS = "es.document.versions"

  protected val log = LoggerFactory.getLogger(this.getClass)

  def newClient(config: Configuration, endpoints: Seq[(String, Int)]) = {
    val settings = ImmutableSettings.settingsBuilder
      .put("client.transport.ignore_cluster_name", true)
      .put("client.transport.sniff", config.getBoolean(ES_CLUSTER_SNIFF, true))
      .build

    val endpointsStr = endpoints.map {
      case (host, port) => s"$host:$port"
    }.mkString(",")

    log.info(s"using ES cluster $endpointsStr")

    endpoints.foldLeft(new TransportClient(settings)) { (cl, hostPort) =>
      val (host, port) = hostPort
      cl.addTransportAddress(new InetSocketTransportAddress(host, port))
    }
  }

  def getTaggedSplits(context: JobContext, tag: String) = {
    val config = context.getConfiguration
    val prefix = if (tag == "") tag else tag + "."

    val endpoints = Option(config.getStrings(prefix+ES_CLUSTER_ENDPOINTS)).getOrElse(Array.empty[String])
      .map(_.split(":"))
      .collect {
      case Array(host, port) => (host, port.toInt)
      case Array(host)       => (host, 9300)
    }

    val client = newClient(context.getConfiguration, endpoints)

    val index = config.get(prefix+ES_INDEX)
    val kind = Option(config.get(prefix+ES_TYPE))
    val query = Option(config.get(prefix+ES_QUERY))
    val includeFields = Option(config.getStrings(prefix+ES_INCLUDE_FIELDS)).getOrElse(Array.empty[String])
    val excludeFields = Option(config.getStrings(prefix+ES_EXCLUDE_FIELDS)).getOrElse(Array.empty[String])
    val versions = config.getBoolean(prefix+ES_DOCUMENT_VERSIONS, true)

    val docTag = if (tag != "") tag else context.getConfiguration.get("es.input.tag", "")

    val shards = client.admin().indices().prepareStatus(index).get().getShards
    shards.filter(_.getShardRouting.primary()).map { ss =>
      val shard = ss.getShardRouting.id
      EsInputSplit(endpoints, index, kind, query, includeFields, excludeFields, shard, versions, docTag).asInstanceOf[InputSplit]
    }
  }
}

case class EsInputSplit(var endpoints: Seq[(String, Int)],
                        var index: String,
                        var kind: Option[String],
                        var query: Option[String],
                        var includeFields: Seq[String],
                        var excludeFields: Seq[String],
                        var shard: Int,
                        var versions: Boolean,
                        var tag: String) extends InputSplit with Writable {
  def this() = this(Seq.empty[(String, Int)], null, None, None, Seq.empty[String], Seq.empty[String], 0, false, null)

  def readFields(in: DataInput) {
    endpoints = Seq.fill(in.readInt())((in.readUTF(), in.readInt()))
    index = in.readUTF()
    kind = in.readUTF() match { case "" => None case e => Some(e) }
    query = in.readUTF() match { case "" => None case e => Some(e) }
    includeFields = Seq.fill(in.readInt())(in.readUTF())
    excludeFields = Seq.fill(in.readInt())(in.readUTF())
    shard = in.readInt()
    versions = in.readBoolean()
    tag = in.readUTF()
  }

  def write(out: DataOutput) {
    out.writeInt(endpoints.size)
    endpoints.foreach { case (host, port) =>
      out.writeUTF(host)
      out.writeInt(port)
    }
    out.writeUTF(index)
    out.writeUTF(kind.getOrElse(""))
    out.writeUTF(query.getOrElse(""))
    out.writeInt(includeFields.size)
    includeFields.foreach(out.writeUTF)
    out.writeInt(excludeFields.size)
    excludeFields.foreach(out.writeUTF)
    out.writeInt(shard)
    out.writeBoolean(versions)
    out.writeUTF(tag)
  }

  def getLength = 0L

  def getLocations = Array.empty[String]
}

class EsRecordReader extends RecordReader[Text, Document] {
  import io.github.dmitrib.ext.hadoop.elasticsearch.EsInputFormat._

  protected val log = LoggerFactory.getLogger(this.getClass)

  var client: TransportClient = _

  var iterator: Iterator[SearchHit] = _

  var processedHits = 0L
  var totalHits = 1L

  var key = new Text()
  var value: Document = _
  var tag: String = _

  def initialize(split: InputSplit, context: TaskAttemptContext) {
    val config = context.getConfiguration

    val hitsPerShard = config.getInt(ES_HITS_PER_SHARD, 1000)
    val requestTimeoutMins = config.getInt(ES_TIMEOUT_MINS, 5)
    val retryCount = config.getInt(ES_RETRY_COUNT, 3)

    val esSplit = split.asInstanceOf[EsInputSplit]

    client = newClient(config, esSplit.endpoints)

    log.info(s"listed cluster nodes: ${client.listedNodes()}")
    log.info(s"connected cluster nodes: ${client.connectedNodes()}")

    tag = esSplit.tag

    val queryBuilder = esSplit.query
      .map(QueryBuilders.queryString)
      .getOrElse(QueryBuilders.matchAllQuery())

    val reqBuilder = client.prepareSearch(esSplit.index)
      .setSearchType(SearchType.SCAN)
      .setScroll(new TimeValue(600000))
      .setQuery(queryBuilder)
      .setSize(hitsPerShard)
      .setPreference(s"_shards:${esSplit.shard}")
      .setTimeout(new TimeValue(requestTimeoutMins, TimeUnit.MINUTES))
      .setVersion(esSplit.versions)

    if (esSplit.excludeFields.nonEmpty || esSplit.includeFields.nonEmpty) {
      reqBuilder.setFetchSource(esSplit.includeFields.toArray, esSplit.excludeFields.toArray)
    }

    esSplit.kind.foreach(reqBuilder.setTypes(_))

    val (it, total) = EsUtil.scan(client, reqBuilder, retryCount, requestTimeoutMins)

    totalHits = total
    iterator = it.flatMap(_.getHits.getHits)
  }

  def nextKeyValue() = {
    if (iterator.hasNext) {
      val resp = iterator.next()
      key = new Text(resp.getId)
      value = Document(resp.getId, resp.getSourceAsString, resp.getVersion, tag)
      processedHits = processedHits + 1
      true
    } else {
      false
    }
  }

  def getCurrentKey = key

  def getCurrentValue = value

  def getProgress = processedHits.toFloat / totalHits

  def close() {
    if (client != null) {
      client.close()
    }
  }
}

class EsInputFormat extends InputFormat[Text, Document] {
  import io.github.dmitrib.ext.hadoop.elasticsearch.EsInputFormat._

  def getSplits(context: JobContext): util.List[InputSplit] = {
    getTaggedSplits(context, "").toList.asJava
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new EsRecordReader
}

case class Document(var id: String, var body: String, var version: Long, var tag: String) extends Writable {
  def this() = this(null, null, 0, null)

  override def readFields(in: DataInput) {
    id = in.readUTF()
    val bodyLength = in.readInt()
    body = if (bodyLength > 0) {
      val bodyBytes = new Array[Byte](bodyLength)
      in.readFully(bodyBytes)
      new String(bodyBytes, "UTF-8")
    } else null
    version = in.readLong()
    tag = in.readUTF()
  }

  override def write(out: DataOutput) {
    out.writeUTF(id)
    val bodyBytes = Option(body).map(_.getBytes("UTF-8")).getOrElse(Array.empty[Byte])
    out.writeInt(bodyBytes.length)
    out.write(bodyBytes)
    out.writeLong(version)
    out.writeUTF(tag)
  }
}

class MultiEsInputFormat extends InputFormat[Text, Document] {
  import io.github.dmitrib.ext.hadoop.elasticsearch.EsInputFormat._

  def getSplits(context: JobContext): util.List[InputSplit] = {
    Option(context.getConfiguration.getStrings("es.input.tags")).getOrElse(Array.empty[String]).flatMap { tag =>
      getTaggedSplits(context, tag)
    }.toList.asJava
  }

  def createRecordReader(split: InputSplit, context: TaskAttemptContext) = new EsRecordReader
}
