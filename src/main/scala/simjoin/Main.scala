package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

object Main {
  def main(args: Array[String]) {     
    
    //val inputFile= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/dblp_2K.csv"
    //val inputFile= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/dblp_4K.csv"
    //val inputFile= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/dblp_6K.csv"
    //val inputFile= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/dblp_8K.csv"
    //val inputFile= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/dblp_10K.csv"
    val inputFile= "./src/main/resources/dblp_2K.csv"
    
    val numAnchors = 10
    
    // distanceThreshold 1 3
    val distanceThreshold = 1
    
    val attrIndex = 0    
        
    //val input = new File(getClass.getResource(inputFile).getFile).getPath    
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)   
    
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(inputFile)      

    val rdd = df.rdd        
    val schema = df.schema.toList.map(x => x.name)    
    val dataset = new Dataset(rdd, schema)           
    
    
    val t1 = System.nanoTime    
    val sj = new SimilarityJoin(numAnchors, distanceThreshold)
    val res = sj.similarity_join(dataset, attrIndex)           

    val t2 = System.nanoTime

    // cartesian
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
                                   .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && Levenshtein0.distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))
    
    val t2Cartesian = System.nanoTime
  }     
}
