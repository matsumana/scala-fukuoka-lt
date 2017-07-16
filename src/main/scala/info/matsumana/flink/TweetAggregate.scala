package info.matsumana.flink

import java.net.{InetAddress, InetSocketAddress}
import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneId}
import java.util.concurrent.TimeUnit
import java.util.{ArrayList, HashMap, Properties}

import com.fasterxml.jackson.core.io.JsonStringEncoder
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.math.ceil

object TweetAggregate {

  val TWITTER_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z uuuu")
  val OUTPUT_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss")
  val TIME_WINDOW = 15

  val mapper = new ObjectMapper()

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromPropertiesFile(args(0))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)

    // source (kafka)
    val kafkaSourceProps = new Properties()
    kafkaSourceProps.setProperty("bootstrap.servers", params.get("bootstrap.servers", "localhost:9092"))
    kafkaSourceProps.setProperty("group.id", params.get("group.id", "scala-fukuoka-2017"))
    val inputTopic = params.get("topic.input", "twitter")
    val consumer = new FlinkKafkaConsumer010[String](
      inputTopic,
      new SimpleStringSchema,
      kafkaSourceProps)
    val sourceStream = env.addSource(consumer)

    // sink (kafka)
    val kafkaSinkProps = new Properties()
    kafkaSinkProps.setProperty("bootstrap.servers", params.get("bootstrap.servers", "localhost:9092"))
    val outputTopic = params.get("topic.output", "twitterAggregated")
    val sinkKafka = new FlinkKafkaProducer010[String](
      outputTopic,
      new SimpleStringSchema,
      kafkaSinkProps)

    // sink (elasticsearch)
    val elasticsearchProps = new HashMap[String, String]
    elasticsearchProps.put("cluster.name", "scala-fukuoka-2017-lt")
    elasticsearchProps.put("bulk.flush.max.actions", "1")
    val transportAddresses = new ArrayList[InetSocketAddress]
    transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
    val sinkElasticsearch = new ElasticsearchSink(elasticsearchProps, transportAddresses,
      new ElasticsearchSinkFunction[String] {
        override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
          val rqst: IndexRequest = Requests.indexRequest
            .index("tweet")
            .`type`("tweet")
            .source(element)
          indexer.add(rqst)
        }
      })

    // stream processing
    val stream = sourceStream
      .map(mapper.readValue(_, classOf[HashMap[String, String]]))
      .map(m => {
        val createdAt = String.valueOf(m.get("created_at"))
        val timestamp = convertTwitterTimestamp(createdAt)
        val text = String.valueOf(m.get("text"))
        val userAny: Any = m.get("user")
        val user = userAny.asInstanceOf[HashMap[String, Object]]
        val screenName = String.valueOf(user.get("screen_name"))
        val name = String.valueOf(user.get("name"))

        (timestamp, screenName, name, text)
      })
      .map(t => {
        val timestamp = t._1
        val screenName = t._2
        val name = t._3
        val text = t._4
        val count = 1

        // TODO 単純にwindow内の件数をカウントしたいだけなので、シンプルなメソッドがあるはず
        //        (timestamp, screenName, name, text, count)
        (timestamp, screenName, name, text, count, 1)
      })

    // to kafka
    val streamKafka = stream
      .keyBy(5) // timestamp  // TODO 単純にwindow内の件数をカウントしたいだけなので、シンプルなメソッドがあるはず
      .timeWindow(Time.of(TIME_WINDOW, TimeUnit.SECONDS))
      .sum(4) // count
      .map(t => ceil(t._5.toDouble / TIME_WINDOW).toInt) // convert to 'per second'
      .map(count => s"""{"count": $count}""")
      .addSink(sinkKafka)

    // to elasticsearch
    val streamElasticsearch = stream
      .map(t => {
        val encoder = JsonStringEncoder.getInstance
        val escapedText = String.valueOf(encoder.quoteAsString(t._4))
        val escapedName = String.valueOf(encoder.quoteAsString(t._3))

        s"""{"created_at": "${t._1}", "screen_name": "${t._2}", "name": "$escapedName", "text": "$escapedText"}"""
      })
      .addSink(sinkElasticsearch)

    env.execute("TweetAggregate")
  }

  def convertTwitterTimestamp(createdAt: String): String = {
    val offsetDateTime = OffsetDateTime.parse(createdAt, TWITTER_DATE_TIME_FORMATTER)
    offsetDateTime
      .atZoneSameInstant(ZoneId.of("Asia/Tokyo"))
      .toLocalDateTime
      .format(OUTPUT_DATE_TIME_FORMATTER)
  }
}
