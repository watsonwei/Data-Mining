import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.ListBuffer
object UniqueUserCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setAppName("UniqueUserCount")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(1))
    val hostname=args(0)
    val port=args(1)
    val lines=ssc.socketTextStream(hostname,port.toInt)
//    val lines=ssc.socketTextStream("localhost",9999)
    var listOfTrailingZeros=List.fill(15)(0)
    lines.foreachRDD(rdd=>{
      for (item<-rdd.collect()){
        val hash_lists=hash(item)  //15 hash functions
        for (i <- hash_lists.indices){
          listOfTrailingZeros=listOfTrailingZeros.updated(i,math.max(listOfTrailingZeros(i),countTrailingZeros(hash_lists(i).toBinaryString)))
        }
      }
      //partition hash functions into 3 groups of size 5, calculate average of every group
      var avgOfGroups=listOfTrailingZeros.map(math.pow(2,_)).grouped(5).toList.map(a=>math.round(a.sum/5)).sorted
      //take the median of averages
      println("Estimated number of unique users= "+avgOfGroups(1))
    })
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }
  def hash(item:String):List[Int]={
    val hash_lists=ListBuffer.empty[Int]
    val number=item.toInt
    var b=2
    var m=10
    for (i<-0 to 14){
      hash_lists+=(i*number+i)%10000
    }
    return hash_lists.toList
  }
  def countTrailingZeros(item:String):Int={
    var count=0
    for(s<-item.reverse){
      if(s=='0'){
        count+=1
      }
      else
        return count
    }
    return count
  }
}
