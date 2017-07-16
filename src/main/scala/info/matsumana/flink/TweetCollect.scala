package info.matsumana.flink

import java.util.regex.Pattern
import java.util.{HashMap, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object TweetCollect {

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
      .map(mapper.readValue(_, classOf[HashMap[String, Object]]))
      .filter(m => {
        val text = m.get("text")
        text != null && TARGET_TWEET_PATTERN.matcher(String.valueOf(text)).matches()
        // TODO ハッシュタグがいっぱい付いてるツイートはスパムなので削除する
      })
      .map(mapper.writeValueAsString(_))
      .addSink(sink)

    env.execute("TweetCollect")
  }
}
