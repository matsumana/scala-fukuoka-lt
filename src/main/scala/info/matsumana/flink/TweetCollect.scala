package info.matsumana.flink

import java.time.format.DateTimeFormatter
import java.time.{OffsetDateTime, ZoneId}
import java.util
import java.util.Properties
import java.util.regex.Pattern

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object TweetCollect {

  val TWITTER_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z uuuu")
  val OUTPUT_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss")
  val DELETED_TWEET_PATTERN = Pattern.compile("""^\{"delete":\{""")
  val TARGET_TWEET_PATTERN = Pattern.compile("^.*[\u3040-\u3096]+.*$")

  val mapper = new ObjectMapper()

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromPropertiesFile(args(0))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(60000)

    // source (twitter)
    val twitterProps = new Properties()
    twitterProps.setProperty(TwitterSource.CONSUMER_KEY, params.get("consumer_key", ""))
    twitterProps.setProperty(TwitterSource.CONSUMER_SECRET, params.get("consumer_secret", ""))
    twitterProps.setProperty(TwitterSource.TOKEN, params.get("token", ""))
    twitterProps.setProperty(TwitterSource.TOKEN_SECRET, params.get("token_secret", ""))
    val sourceStream = env.addSource(new TwitterSource(twitterProps))

    // sink (kafka)
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", params.get("bootstrap.servers", "localhost:9092"))
    val topic = params.get("topic", "twitter")
    val sink = new FlinkKafkaProducer010[String](
      topic,
      new SimpleStringSchema,
      kafkaProps)

    // stream processing
    sourceStream
      .filter(!DELETED_TWEET_PATTERN.matcher(_).matches())
      .map(mapper.readValue(_, classOf[util.HashMap[String, Object]]))
      .filter(m => {
        val text = m.get("text")
        text != null && TARGET_TWEET_PATTERN.matcher(String.valueOf(text)).matches()
        // TODO ハッシュタグがいっぱい付いてるツイートはスパムなので削除する
      })
      .map(m => {
        val createdAt = String.valueOf(m.get("created_at"))
        val timestamp = convertTwitterTimestamp(createdAt)
        val text = String.valueOf(m.get("text"))
        val userAny: Any = m.get("user")
        val user = userAny.asInstanceOf[util.HashMap[String, Object]]
        val screenName = user.get("screen_name")
        val name = user.get("name")

        s"""{"created_at": "$timestamp", "screen_name": "$screenName", "name": "$name", "text": "$text"}"""
      })
      .addSink(sink)

    env.execute("TweetCollect")
  }

  def convertTwitterTimestamp(createdAt: String): String = {
    val offsetDateTime = OffsetDateTime.parse(createdAt, TWITTER_DATE_TIME_FORMATTER)
    offsetDateTime
      .atZoneSameInstant(ZoneId.of("Asia/Tokyo"))
      .toLocalDateTime
      .format(OUTPUT_DATE_TIME_FORMATTER)
  }
}
