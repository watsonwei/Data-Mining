import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import java.io._
import java.util.Collections
object Son {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("task1").setMaster("local[2]")
    val sc=new SparkContext(conf)
    var case_number=args(0)
    val file=args(1)
    val support=args(2).toDouble
    val lines = sc.textFile(file)
    var pairs:RDD[Tuple2[String,String]]=null
    if(case_number == "1"){
      pairs=lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(line=>line.split(",")).map((array: Array[String])=>(array(0),array(1)))
    }
    else if(case_number== "2"){
      pairs=lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(line=>line.split(",")).map((array: Array[String])=>(array(1),array(0)))
    }
    var baskets=pairs.groupByKey().map{case(a,b)=>b.toSet}
    var num_partitions=baskets.partitions.size
    var map_1=baskets.mapPartitions(lists => apriori(lists, support/num_partitions)).map(a=>(a,1))
    var candidates=map_1.map(a=>a._1).collect().toSet
    var map_2=baskets.flatMap(basket=>{
      var set=Set.empty[Tuple2[Set[String], Int]]
      for(candidate<-candidates){
        if(candidate.subsetOf(basket.toSet)){
          set+=Tuple2(candidate, 1)
        }
      }
      set
    })
    var reduce_2=map_2.reduceByKey((a,b)=>a+b)
    var result = reduce_2
      .filter(item=> (item._2 >= support))
      .map { case (a,b) => (a.size, List(a.toList.sorted)) }.reduceByKey((a:List[List[String]],b:List[List[String]])=>a++:b).sortBy(_._1).map{case(a,b)=>{
      var list=List.empty[String]
      for(item<-b){
        var newitem=List.empty[String]
        for(ele<-item){
          newitem=("'"+ele+"'")::newitem
        }
        list=newitem.reverse.mkString("(",",",")")::list
      }
      list
    }}.map(list=>list.sorted)
    var output_file:String=""
    if(file=="beauty.csv"||file=="books.csv"){
      output_file="Yi_Wei_SON_"+file.split("\\.")(0).capitalize+".case"+case_number+"-"+support.toInt.toString+".txt"
    }
    else{
      output_file="Yi_Wei_SON_"+file.split("\\.")(0).capitalize+".case"+case_number+".txt"
    }
    var pw = new PrintWriter(new File(output_file))
    for(item<-result.collect()){
      pw.write(item.mkString(","))
      pw.write("\n\n")
    }
    pw.close
  }
  def getCandidates(freqItemSets: Set[Set[String]], k: Int): Set[Set[String]] = {
    var singles=Set.empty[String]
    for(itemSets<-freqItemSets){
      for(item<-itemSets){
        singles+=item
      }
    }
    var result=Set.empty[Set[String]]
    for(itemSets<-freqItemSets){
      for(single<-singles){
        var newitemSet=Set()++itemSets
        newitemSet+=single
        if(newitemSet.size==k){
          var judge=true
          if(k>2){
            var list_check=newitemSet.subsets(k-1).toList
            for(item<-list_check){
              if(!freqItemSets.contains(item)){
                judge=false
              }
            }
          }
          if(judge){
            result+=newitemSet
          }
        }
      }
    }
    return result
  }

  def getFrequent(candidates: Set[Set[String]], baskets:List[Set[String]], threshold: Double): Set[Set[String]] = {
    var map=HashMap.empty[Set[String],Int]
    var freq_set=Set.empty[Set[String]]
    for(basket<-baskets){
      for(candidate<-candidates){
        if(candidate.subsetOf(basket)){
          if(map.contains(candidate)){
            map(candidate)+=1
          }
          else{
            map(candidate)=1
          }
        }
      }
    }
    for(pair<-map){
      if(pair._2>=threshold){
        freq_set+=pair._1
      }
    }
    return freq_set
  }

  def apriori(baskets: Iterator[Set[String]], threshold: Double):Iterator[Set[String]]={
    var single_map = HashMap.empty[String,Int]
    var freq_set=Set.empty[Set[String]]
    var whole_freq_set=Set.empty[Set[String]]
    var newbaskets=baskets.toList
    for(basket<-newbaskets){
      for(item<-basket){
        if(single_map.contains(item)){
          single_map(item)+=1
        }
        else{
          single_map += (item -> 1)
        }
      }
    }
    for(pair<-single_map){
      if(pair._2>=threshold){
        freq_set+=Set(pair._1)
      }
    }
    whole_freq_set=whole_freq_set++freq_set
    var k=2
    while(!freq_set.isEmpty){
      var candidates=getCandidates(freq_set,k)
      var new_freq_set=getFrequent(candidates,newbaskets,threshold)
      if(!new_freq_set.isEmpty){
        whole_freq_set=whole_freq_set++new_freq_set
        k+=1
      }
      freq_set=new_freq_set
    }
    return whole_freq_set.iterator

  }
}
