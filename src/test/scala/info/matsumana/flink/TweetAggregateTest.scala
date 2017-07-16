package info.matsumana.flink

import com.fasterxml.jackson.core.io.JsonStringEncoder
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TweetAggregateTest extends FlatSpec {

  it should "convertTwitterTimestamp" in {
    val createdAt = "Sat Jul 15 16:54:53 +0000 2017"
    val timestamp = TweetAggregate.convertTwitterTimestamp(createdAt)
    assert(timestamp === "2017/07/16 01:54:53")
  }

  it should "espace json back slash" in {
    val source = """\(´°v°)/"""
    val encoder = JsonStringEncoder.getInstance
    val escapedText = String.valueOf(encoder.quoteAsString(source))
    assert(escapedText === """\\(´°v°)/""")
  }

  it should "espace json double quote" in {
    val source = """実験！EOS8000Dは32"MB"のSDカードで何枚撮影出来るのか！？ https://t.co/Z4UzS8Gssi"""
    val encoder = JsonStringEncoder.getInstance
    val escapedText = String.valueOf(encoder.quoteAsString(source))
    assert(escapedText === """実験！EOS8000Dは32\"MB\"のSDカードで何枚撮影出来るのか！？ https://t.co/Z4UzS8Gssi""")
  }
}
