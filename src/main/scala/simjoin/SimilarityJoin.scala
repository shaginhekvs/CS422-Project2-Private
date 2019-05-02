package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer

object Levenshtein0 extends App {
 
  def distance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }
 
    @inline
    def minimum(i: Int*): Int = i.min
 
    for {j <- dist.indices.tail
         i <- dist(0).indices.tail} dist(j)(i) =
        if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
        else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
 
    dist(s2.length)(s1.length)
  }
 
  def printDistance(s1: String, s2: String) {
    println("%s -> %s : %d".format(s1, s2, distance(s1, s2)))
  }
 
  printDistance("kitten", "sitting")
  printDistance("rosettacode", "raisethysword")
 
}

object MyFunctions {
  
  
  def filterVals(anchors :Array[Row],currentRow:Row,attrIndex : Int,d:Int): ListBuffer[(Int,(Int,Row))] = {
    var mapKey = collection.mutable.Map[Int,Int]();
    for((x,i) <- anchors.view.zipWithIndex){
    mapKey +=  i-> Levenshtein0.distance(currentRow.get(attrIndex).toString,x.get(attrIndex).toString);
    }
    val homeAnchor = mapKey.minBy(_._2)._1; // get Home Anchor of this row
    val outer_anchors = mapKey.filter(x=>x._2 <= mapKey(homeAnchor)+ 2 * d);
    
    var curList = new ListBuffer[(Int,(Int,Row))](); 
    curList += ((homeAnchor, (0,currentRow)));
    outer_anchors.foreach((k)=>curList += ((k._1,(1,currentRow)))) // more optimization possible acc to hashing , not sure if it works though
    
    return curList;
    }
  def verify(key:Int , it:Iterable[(Int,Row)],attrIndex : Int , d:Int ):ListBuffer[(Row,Row)] = {
    val rows = it.toList
    var homeList = new ListBuffer[Row]();
    var outerList = new ListBuffer[Row]();
    for (row <- rows){
      if(row._1 == 0)homeList += (row._2);
      else outerList += (row._2);
    }
    var closeEnough = new ListBuffer[(Row,Row)]();
    for((home1,i)<- homeList.view.zipWithIndex ){
      for ((home2,j) <- homeList.view.zipWithIndex ){
         if(j>i){
           if(Levenshtein0.distance(home1.get(attrIndex).toString,home2.get(attrIndex).toString)<= d){
             closeEnough += ((home1,home2))
           }
         }
      }   
    }
    for((outer1,i)<- outerList.view.zipWithIndex ){
      for ((home1,j) <- homeList.view.zipWithIndex ){
         if(Levenshtein0.distance(home1.get(attrIndex).toString,outer1.get(attrIndex).toString)<= d){
           closeEnough += ((outer1,home1))
         }
      }   
    }
    
    return closeEnough;
  }
}


class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  
  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    
    val rdd = dataset.getRDD()
    
    // step 1: sampling
    val schema = dataset.getSchema()
    val overallRowSize = rdd.count().toInt
    val fract = (numAnchors*1.0)/overallRowSize;
    val rdd_anchors = rdd.sample(false, fract, 100);
    val rdd_anchors_array = rdd_anchors.collect();
    // step 2 : filtering
    val rdd_filtered = rdd.flatMap(x=>MyFunctions.filterVals(rdd_anchors_array,x,attrIndex,distThreshold));
    //val x = rdd_filtered.take(10).map(println )
     //step 3 : verification
    val rdd_reduced = rdd_filtered.groupByKey();
    val rdd_verified = rdd_reduced.flatMap(x=>MyFunctions.verify(x._1,x._2,attrIndex,distThreshold))
    
    rdd_verified.take(10).map(println );
    println(overallRowSize)
    
    null
  }
}

