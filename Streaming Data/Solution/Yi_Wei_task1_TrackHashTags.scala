import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
/**
  * Created by lnahoom on 22/08/2016.
  */
object TrackHashTags {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setAppName("TrackHashTags")
    conf.setMaster("local[2]")
    val consumerKey=args(0)
    val consumerSecret=args(1)
    val accessToken=args(2)
    val accessTokenSecret=args(3)
//    val consumerKey="NW3PXmfvfQYcyTO5mpU79hygB"
//    val consumerSecret="VOuK9zrQY9a4peVb687VALNBNCZzz6E0E2eX1fVwZkkSrDNAJi"
//    val accessToken="835545284-m8v7ga1X0BUDwFtOemz23XXDEui9qy9W9ennxulJ"
//    val accessTokenSecret="BBqQ2z2COUCMAlyTWTQrmdUQdZUDDkI4qQDSKHlc7XSnZ"
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    val ssc = new StreamingContext(conf, Seconds(2))
//    println(ssc)
    val stream = TwitterUtils.createStream(ssc, None).filter(_.getLang != "ar")
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
    val topCounts5 = hashTags.map((_, 1)).reduceByKeyAndWindow((a:Int,b:Int) => (a + b),Seconds(120),Seconds(2))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))
    topCounts5.foreachRDD(rdd => {
      val topList = rdd.take(5)
      if(topList.length!=0){
        println("\nTop 5 popular hash tags in last 2 minutes:")
        topList.foreach{case (count, tag) => println("%s, count: %s".format(tag, count))}
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}
