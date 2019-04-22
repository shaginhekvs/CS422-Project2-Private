package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import org.slf4j.Logger
import org.slf4j.LoggerFactory

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

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  
  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    // step 1: sampling
    
    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()
    val overallRowSize = rdd.count().toInt
    val fract = (numAnchors*1.0)/overallRowSize;
    val rdd_anchors = rdd.sample(false, fract, 100);
    rdd_anchors.collect().map(println );
    println(overallRowSize)
    
    null
  }
}

