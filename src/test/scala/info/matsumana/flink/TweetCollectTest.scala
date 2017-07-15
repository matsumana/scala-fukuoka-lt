package info.matsumana.flink

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TweetCollectTest extends FlatSpec {

  it should "convertTwitterTimestamp" in {
    val createdAt = "Sat Jul 15 16:54:53 +0000 2017"
    val timestamp = TweetCollect.convertTwitterTimestamp(createdAt)
    assert(timestamp === "2017/07/16 01:54:53")
  }
}
