import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
object ModelBasedCF {
  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("target")
    // Load and parse the data
//    val data = sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment_3/Data/video_small_num.csv")
    val data = sc.textFile(args(0))
//    val test_data = sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment_3/Data/video_small_testing_num.csv")
    val test_data = sc.textFile(args(1))

    val test_ratings = test_data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(',') match { case Array(user, item, rate) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    })
//    ratings=sc.parallelize(ratings.collect().toSet.diff(test_ratings.collect().toSet).toList)

    // Evaluate the model on rating data
    val usersProducts = test_ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }
    val s=usersProducts.collect().toSet
    var ratings = data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(',') match { case Array(user, item, rate, timestamp) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    }).filter(x=> (s((x.user, x.product))==false))
//    println(ratings.collect().size)
    // Build the recommendation model using ALS
    val rank = 5
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.25)

    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }.sortByKey()
    val ratings_predicted=predictions.map(a=>a._2).collect().toList.sorted
    val min=ratings_predicted(0)
    val max=ratings_predicted.last
//    println("max "+ max)
//    println("min" + min)
    val normalize=predictions.map(a=>{
      val newrate=(a._2-min)/(max-min)*4+1
//      println(newrate)
      ((a._1._1,a._1._2),newrate)
//      if (newrate>=0&&newrate<0.2) ((a._1._1,a._1._2),1)
//      else if (newrate>=0.2&&newrate<0.4) ((a._1._1,a._1._2),2)
//      else if (newrate>=0.4&&newrate<0.6) ((a._1._1,a._1._2),3)
//      else if (newrate>=0.6&&newrate<0.9) ((a._1._1,a._1._2),4)
//      else ((a._1._1,a._1._2),5)
    })
//    var pw = new PrintWriter(new File("Yi_Wei_ModelBasedCF.txt"))
    var pw = new PrintWriter(new File(args(2)))
    for(re<-normalize.collect()){
      pw.write(re._1._1+","+re._1._2+","+re._2)
      pw.write("\n")
//      println(re)
    }
    pw.close
//    for (item<-normalize.collect()){
//      println(item)
//    }
    val ratesAndPreds = test_ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(normalize)
//    for (item<-ratesAndPreds.collect()){
//      println(item)
//    }
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
}

