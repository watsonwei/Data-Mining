import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
object JaccardLSH {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val conf = new SparkConf()
    conf.setAppName("JaccardLSH")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val lines = sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment_3/Data/video_small_num.csv")
    val lines = sc.textFile(args(0))
    val pairs=lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(line=>line.split(",")).map((array: Array[String])=>(array(1).toInt,array(0).toInt))
    val baskets=pairs.groupByKey().sortByKey()
    val users=baskets.map{case(a,b)=>b.toList}.collect().toList
    var signatures=baskets.map(basket=>{
      var sigs=ListBuffer.empty[Int]
      for(i<-1 to 200){
        var sig=getsig(i,basket._2.toList)
        sigs+=sig
      }
  (basket._1,sigs.toList)
    })
    var candidates=signatures.flatMap(signature=>{
      var bands=signature._2.grouped(20).toList
      var temp=ListBuffer.empty[Tuple2[List[Int],Int]]
      var band_index=0
      for(band<-bands){
        temp+=Tuple2(band_index::band,signature._1)
        band_index+=1
      }
      temp.toList
    }).groupByKey().filter(a=>(a._2.size>=2)).flatMap(a=>{
      a._2.toList.sorted.combinations(2).toList
    }).collect().toSet

    val result=sc.parallelize(candidates.toList).map(candidate=>((candidate(0),candidate(1)),jaccard(users(candidate(0)),users(candidate(1))))).filter(a=>(a._2>=0.5)).sortByKey()
//    println(result.collect().size)

//    val truth = sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment_3/Data/video_small_ground_truth_jaccard.csv")
//    val truth_pairs=truth.map(line=>line.split(",")).map((array: Array[String])=>(array(0).toInt,array(1).toInt)).collect().toSet
//    var real_number=0
//    var pw = new PrintWriter(new File("output.txt"))
    var pw = new PrintWriter(new File(args(1)))
    for(re<-result.collect()){
      pw.write(re._1._1+","+re._1._2+","+re._2)
      pw.write("\n")
//      if (truth_pairs(re._1)) real_number+=1
//      println(re)
    }
    pw.close
//    println("precision"+real_number.toDouble/result.collect().size)
//    println("recall"+real_number.toDouble/truth_pairs.size)
//    val duration = (System.nanoTime - t1) / 1e9d
//    println(duration)
  }
  def jaccard(l1:List[Int],l2:List[Int]):Double={
    val union=l1.toSet.union(l2.toSet).size.toDouble
    val intersection=l1.toSet.intersect(l2.toSet).size.toDouble
    return intersection/union
  }
  def getsig(round:Int,users:List[Int]):Int={
    var a=1
    var b=round
    var m=200
    var newusers=ListBuffer.empty[Int]
    for(user<-users){
      newusers+=(a*user+b)%m
    }
    newusers.toList.min
  }
}
