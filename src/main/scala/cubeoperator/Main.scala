package cubeoperator
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

object Main {
  def main(args: Array[String]) {
    val reducers = 10

    //val inputFile= "../lineorder_small.tbl"
    //val inputFile = "//cs422-data/tpch/sf100/parquet/lineitem.parquet"
    //val input = new File(getClass.getResource(inputFile).getFile).getPath
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
/*
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)
      sqlContext.read
*/  
    /* 
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(input)
    */
    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .parquet("/cs422-data/tpch/sf100/parquet/lineitem.parquet")
    
    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")
    println("fast")
    val res = cb.cube(dataset, groupingList, "lo_supplycost", "AVG")
    println("naive")
    val res2 = cb.cube_naive(dataset, groupingList, "lo_supplycost", "AVG")

    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */


    //Perform the same query using SparkSQL

        val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
          .agg(sum("lo_supplycost") as "avg supplycost");
        q1.show

       //println("cube here");
  }
}