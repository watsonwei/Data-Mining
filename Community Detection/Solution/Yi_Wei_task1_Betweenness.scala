import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object Betweenness {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Datasets Test")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ratings_rdd=sc.textFile(args(0))
//    val ratings_rdd=sc.textFile("/Users/watson/Desktop/inf553_hw/Assignment4/Data/video_small_num.csv")
    val itemUser=ratings_rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map(_.split(',') match { case Array(user, item, rate, timestamp) =>
      (item.toInt,user.toInt)
    })
    val edges=itemUser.groupByKey().map(a=>a._2).flatMap(a=>a.toList.sorted.combinations(2)).map(a=>(a,1)).reduceByKey(_+_).filter(a=>(a._2>=7)).map(a=>a._1).map(a=>(a(0),a(1)))
//    for(edge<-edges){
//      println(edge)
//    }
//    println("edges"+edges.collect().size)
    edges.persist()
    val vertices=edges.flatMap(a=>List(a._1,a._2)).distinct()
//    println(vertices.collect().toList.size)
    val adj=edges.union(edges.map(a=>(a._2,a._1))).groupByKey().collectAsMap()
//    for(item<-adj){
//      println(item)
//    }
//val vertices = sc.parallelize(Seq(8))

//    val adj = mutable.Map[Int, List[Int]]()
//    adj.put(0, List(1, 2))
//    adj.put(1, List(0, 2, 3))
//    adj.put(2, List(0, 1))
//    adj.put(3, List(1, 4, 5, 6))
//    adj.put(4, List(3, 5))
//    adj.put(5, List(3, 4, 6))
//    adj.put(6, List(3, 5))
//    adj.put(7, List(8))
//    adj.put(8, List(4,7))
    val betweenness=vertices.flatMap(vertice=>calBetweenness(adj,vertice)).map{case((parent,child),credit)=>{
  if (parent>child) ((child,parent),credit)
  else ((parent,child),credit)
}}.reduceByKey(_+_).map{case((parent,child),credit)=>((parent,child),credit/2)}.sortByKey().map{case((parent,child),credit)=>(parent,child,credit)}
//    for (item<-betweenness.collect()){
//      println(item)
//    }
    var pw = new PrintWriter(new File(args(1)+"Yi_Wei_Betweenness.txt"))
//    var pw = new PrintWriter(new File("Yi_Wei_Betweenness.txt"))
    for(re<-betweenness.collect()){
      pw.write("("+re._1+","+re._2+","+re._3+")")
      pw.write("\n")
    }
    pw.close
//    println(betweenness.collect().size)
  }
  def calBetweenness(adj: collection.Map[Int, Iterable[Int]], root: Int): List[Tuple2[(Int, Int), Double]] ={
    var q=mutable.Queue.empty[Int]
    var stack=mutable.Stack[Int]()
    var parents=mutable.Map.empty[Int,Set[Int]]
    var numOfPaths=mutable.Map.empty[Int,Int]
    var traversedNodes=Set.empty[Int]
    q.enqueue(root)
    parents(root)=Set.empty[Int]
    numOfPaths(root)=1
    traversedNodes+=root
    while(q.nonEmpty){
      var neighbours=Set.empty[Int]
      for (i<- 1 to q.size){
        val parent=q.dequeue()
        for (neighbour<-adj.getOrElse(parent,List.empty[Int])){
          if (!traversedNodes(neighbour)){
            var parentsOfNeighbour=parents.getOrElse(neighbour,Set.empty[Int])
            parentsOfNeighbour+=parent
            parents(neighbour)=parentsOfNeighbour
            if (numOfPaths.contains(neighbour)){
              numOfPaths(neighbour)=numOfPaths.getOrElse(parent,0)+numOfPaths.getOrElse(neighbour,0)
            }
            else numOfPaths(neighbour)=numOfPaths.getOrElse(parent,0)
            if (!neighbours(neighbour)){
              q.enqueue(neighbour)
              stack.push(neighbour)
            }
            neighbours+=neighbour
          }
        }
      }
      traversedNodes=traversedNodes.union(neighbours)
    }
    var credits=mutable.Map.empty[Int,ListBuffer[Tuple2[Int,Double]]]
    var i=1
    while(stack.nonEmpty){
      val child=stack.pop()
      val creditsFromChild=credits.getOrElse(child,ListBuffer.empty[Tuple2[Int,Double]]).map(_._2).sum
      val parentsOfchild=parents.getOrElse(child,Set.empty[Int])
//      println(child+" "+creditsFromChild)
      for (parent<-parentsOfchild){
        val weight=numOfPaths.getOrElse(parent,1)/numOfPaths.getOrElse(child,1).toDouble
//        println(child+"weight "+weight)
        val bwsOfChild=(1+creditsFromChild)*weight
//        println(child+"bwsofchild "+bwsOfChild)
        val bwsOfParent=credits.getOrElse(parent,ListBuffer.empty[Tuple2[Int,Double]])
        bwsOfParent+=Tuple2(child,bwsOfChild)
        credits(parent)=bwsOfParent
//        println(child+"->"+credits.get(parent))
        i+=1
      }
    }
    credits.flatMap{case(parents,creditsfromchild)=>creditsfromchild.map{case(child,credit)=>((parents,child),credit)}}.toList
  }
}

