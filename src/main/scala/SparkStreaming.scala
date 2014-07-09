//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

// This Demo requires file root/etc/twitter.txt.
// The contents of this file must include your twitter credentials like below.

// consumerKey = jnoX1xxxxxxxx...
// consumerSecret = eDYCYVYMTx...
// accessToken = 352827242xxxx...
// accessTokenSecret = EiyUeHc...

//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::
//::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import util.TwitterHelper


object SparkStreaming extends App {

  val happyWords = Set("happy", "love", "laugh", "excited")

  val ssc = new StreamingContext(
    master = "local[4]",
    appName = "SparkStreaming",
    batchDuration = Seconds(1)
  )

  TwitterHelper.configureTwitterCredentials()

  val tweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)
  val statuses: DStream[String] = tweets.map(status => status.getText)

  def filterTweetsWithWords(filterWords: Set[String], statuses: DStream[String]) = statuses.filter { status =>
    !status.split(" ").filter{ word => filterWords.contains(word.toLowerCase) }.isEmpty
  }

  val happyTweets = filterTweetsWithWords(happyWords, statuses)

  happyTweets.foreachRDD(rdd => println(s"${rdd.take(10).mkString("\n")}\n\n"))

  ssc.start()
  ssc.awaitTermination()
}
