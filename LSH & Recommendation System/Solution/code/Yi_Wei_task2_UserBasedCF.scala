import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
object UserBasedCF {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
//    val data = sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment_3/Data/video_small_num.csv")
    val data = sc.textFile(args(0))
//    val test_data = sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment_3/Data/video_small_testing_num.csv")
    val test_data = sc.textFile(args(1))
    val ratings = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(',') match { case Array(user, item, rate, timestamp) =>
      ((user.toInt, item.toInt), rate.toDouble)
    })
    val test_ratings = test_data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(',') match { case Array(user, item, rate) =>
      ((user.toInt, item.toInt),rate.toDouble)
    })
    val usersProducts = test_ratings.map { case ((user, product), rate) =>
      (user, product)
    }
    val training=ratings.subtractByKey(test_ratings).map{case((user,item),rate)=>(user,item,rate)}
    val user_item_rate=training.map{case(user,item,rate)=>(user,(item,rate))}.groupByKey()
    val item_user_rate=training.map{case(user,item,rate)=>(item,(user,rate))}.groupByKey()
    val item_user=training.map{case(user,item,rate)=>(item,user)}.groupByKey()
    val test_item_user=test_ratings.map{case((user,item),rate)=>(item,user)}
    val rows=test_item_user.join(item_user).flatMap{case(item,(i,users))=>
        var user_list=ListBuffer.empty[Int]
        user_list+=i
        user_list++=users
        user_list.toList
    }.distinct().map(user=>(user,1)).join(user_item_rate).map{case(user,(1,itemRates))=>(user,itemRates)}.sortByKey().collectAsMap()
    val result=test_item_user.join(item_user_rate).map{case(item,(i,userRates))=>
        val users=userRates.map(a=>a._1).toList
        var l=ListBuffer.empty[Tuple2[Int,List[Double]]]
        var row_1=rows.getOrElse(i,List())
      val other_ratings_1=row_1.map(a=>a._2)
      val avg_1=other_ratings_1.sum/other_ratings_1.size
        for(j<-users){
          val row_2=rows.getOrElse(j,List())
          val other_ratings_2=row_2.filter(a=>(a._1!=item)).map(a=>a._2)
          val avg_2=other_ratings_2.sum/other_ratings_2.size
//          if(corr.contains((i,j))) l+=Tuple2(j,List(corr.getOrElse((i,j),0.0),avg_2))
//          else if (corr.contains((j,i))) l+=Tuple2(j,List(corr.getOrElse((i,j),0.0),avg_2))
//          else{
//            println("row_1",row_1)
//            println("row_2",row_2)
            val similarity=pearson_corr(row_1.toList,row_2.toList)
            l+=Tuple2(j,List(similarity,avg_2))
//            println(similarity)
//            if(i<j) corr.put((i,j),similarity)
//            else corr.put((j,i),similarity)
//          }
        }
//      l = l.sortBy(_._2(0))(Ordering[Double].reverse).slice(0, Math.min(l.size, 10))
      val userToRate=userRates.toMap
      val up = l.map{ case(user, w) => (userToRate(user)-w(1))*w(0)}.sum
      val down=l.map{case(user,w)=>Math.abs(w(0))}.sum
      var predict=0.0
      if(down!=0)
        predict=avg_1+up/down
      else
        predict = userRates.map(_._2).sum / userRates.size
      if(predict<0)
        ((i,item),0.0)
      else if (predict>5)
        ((i,item),5.0)
      else ((i,item),predict)
    }.sortByKey()
//    var pw = new PrintWriter(new File("Yi_Wei_UserBasedCF.txt"))
    var pw = new PrintWriter(new File(args(2)))
    var count=0
    for(re<-result.collect()){
      pw.write(re._1._1+","+re._1._2+","+re._2)
      pw.write("\n")
    }
    pw.close
    val ratesAndPreds=result.join(test_ratings)
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    val level = ratesAndPreds.map{
      case ((user, product), (r1, r2)) =>{
        if(Math.abs(r1-r2)<1&&Math.abs(r1-r2)>=0) (1,1)
        else if(Math.abs(r1-r2)<2&&Math.abs(r1-r2)>=1) (2,1)
        else if(Math.abs(r1-r2)<3&&Math.abs(r1-r2)>=2) (3,1)
        else if(Math.abs(r1-r2)<4&&Math.abs(r1-r2)>=3) (4,1)
        else (5,1)
      }
    }.reduceByKey(_+_)
    val level_map=level.collect().toMap
    println(">=0 and <1: "+level_map.getOrElse(1,0))
    println(">=1 and <2: "+level_map.getOrElse(2,0))
    println(">=2 and <3: "+level_map.getOrElse(3,0))
    println(">=3 and <4: "+level_map.getOrElse(4,0))
    println(">=4: "+level_map.getOrElse(5,0))
    println("RMSE: "+math.sqrt(MSE))
    val duration = (System.nanoTime - t1) / 1e9d
    println("Time: "+duration+" sec")
  }
  def pearson_corr(row_1: List[(Int, Double)] , row_2: List[(Int, Double)]): Double ={
    var pairs_corated=ListBuffer.empty[Tuple2[Double,Double]]
    for(itemRates_1<-row_1){
      for(itemRates_2<-row_2){
        if(itemRates_1._1==itemRates_2._1)
          pairs_corated+=Tuple2(itemRates_1._2,itemRates_2._2)
      }
    }
    if(pairs_corated.size>0)
      {
        val avg_1=pairs_corated.map(a=>a._1).sum/pairs_corated.size
        val avg_2=pairs_corated.map(a=>a._2).sum/pairs_corated.size
        val up=pairs_corated.map{case(a,b)=>(a-avg_1)*(b-avg_2)}.sum
        val square_1=pairs_corated.map(a=>(a._1-avg_1)*(a._1-avg_1)).sum
        val square_2=pairs_corated.map(a=>(a._2-avg_2)*(a._2-avg_2)).sum
        if(square_1!=0&&square_2!=0) return up/(Math.sqrt(square_1)*Math.sqrt(square_2))
        else return 0.0
      }
    else
      return 0.0

  }
}
