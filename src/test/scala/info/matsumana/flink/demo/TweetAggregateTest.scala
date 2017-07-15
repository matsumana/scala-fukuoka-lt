package info.matsumana.flink.demo

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
}