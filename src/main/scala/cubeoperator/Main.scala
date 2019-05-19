package cubeoperator
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.sql.{Row, SparkSession}



object Main {
  def main(args: Array[String]) {
    
    val reducers = 16

    // Local
    
    val inputFile= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_medium.tbl"
    //val inputFile2= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_medium_half.tbl"
    //val inputFile= "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\CS422-Project2-Private\\src\\main\\resources\\lineorder_small.tbl"
    
    
    // CLuster
    
    //val inputFile = "/cs422-data/tpch/sf100/parquet/lineitem.parquet"
    //val inputFile = "/cs422-data/tpch/sf100/parquet/lineitem.parquet"        
    //val input = new File(getClass.getResource(inputFile).getFile).getPath
    //val sparkConf = new SparkConf().setAppName("CS422-Project2")
    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
  

    val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "|")
    .load(inputFile)
      
    val rdd = df.rdd
    val rdd_row = rdd.take(1) 
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(rdd_row)
     
    val schema = df.schema.toList.map(x => x.name) 
   
    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    
    // Local test   Test different number of cube attributes
    
    //var groupingList = List("lo_suppkey","lo_shipmode")
    //var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")
    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate", "lo_supplycost")
    
    
    // Cluster test
    
    //var groupingList = List("l_suppkey","l_shipmode","l_shipdate")
    
    val res = cb.cube(dataset, groupingList, "lo_quantity", "MIN")

    val res2 = cb.cube_naive(dataset, groupingList, "lo_quantity", "MIN")

    //Perform query using SparkSQL
    val tsql = System.currentTimeMillis()
    //val q1 = df.cube("lo_suppkey","lo_shipmode")
    //val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate", "lo_supplycost")
      .agg(sum("lo_quantity") as "avg quantity");

    val duration = (System.currentTimeMillis() - tsql) / 1000.0

  }
}