import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.log4j.{Level, Logger}
/**
  * Created by lnahoom on 22/08/2016.
  */
object AveTweetLength {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setAppName("AveTweetLength")
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
    val stream = TwitterUtils.createStream(ssc, None)
//    val hashTags = stream.flatMap(status => {status.getText.split(" ").filter(_.startsWith("#"))})
    val lengths = stream.map(status => status.getText.length)
    var count=0.0
    var sum=0.0
    lengths.foreachRDD(rdd=>{
      if(rdd.count()!=0){
        count+=rdd.count()
        sum+=rdd.sum
        println("Total tweets: "+count.toInt+", Average length: "+sum/count)
      }
    })
    ssc.start()
    ssc.awaitTermination()

  }
}

