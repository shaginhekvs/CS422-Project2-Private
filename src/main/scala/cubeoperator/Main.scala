package cubeoperator
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._
import org.apache.spark.sql.Row


object MyFunctions2 {
  
  def genMap(indicesToKeep:List[Int],currentRow:Row): collection.mutable.Map[Int,Any] = {
    var mapKey = collection.mutable.Map[Int,Any]();
    println(currentRow);
    indicesToKeep.foreach( x=> mapKey +=  (x-> currentRow.get(x)));
    return mapKey
  }
}
object Main {
  def main(args: Array[String]) {
    val reducers = 10

    val inputFile= "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\CS422-Project2-Private\\src\\main\\resources\\lineorder_small.tbl"
    
    // option 1 
    //val inputFile = "/cs422-data/tpch/sf100/parquet/lineitem.parquet"
    //val inputFile = "/cs422-data/tpch/sf100/parquet/lineitem.parquet"        
    //val input = new File(getClass.getResource(inputFile).getFile).getPath
    //val sparkConf = new SparkConf().setAppName("CS422-Project2")
    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
   //val df = sqlContext.read.option("delimiter", "|").parquet(inputFile);
  
  val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile) 
      
     val rdd = df.rdd
    //val rdd = dataset.getRDD()
    val nh = 1;
    val schema = df.schema.toList.map(x => x.name)
    var groupingAttributes = List("lo_suppkey","lo_shipmode","lo_orderdate")
    println("Schema is below")
    println(schema)
    val index = groupingAttributes.map(x => schema.indexOf(x))
    println(index)
    val keyedRow = rdd.map(x=>(MyFunctions.genMap(index,x),x));
    val keyed = rdd.map(x=>(MyFunctions.genMap(index,x),1));
    val counts = keyed.reduceByKey(_+_).map(x=>(x._1,if (x._2>nh) nh*1.0/x._2 else 1.0));
    val res = counts.collectAsMap()
    val subsam = keyedRow.sampleByKeyExact(false,res,seed = 100);
    subsam.take(10).map(println)
    println(subsam.count())
    println(rdd.count())
     /*
    val rdd = df.rdd
    println("---RDD ---");
    rdd.take(1000).map(println );
    println("---RDD done ---");
    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")
    //var groupingList = List("l_suppkey","l_shipmode","l_shipdate")
    println("fast")
    val res = cb.cube(dataset, groupingList, "l_quantity", "AVG")
    println("naive")
    val res2 = cb.cube_naive(dataset, groupingList, "l_quantity", "AVG")

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
        * 
        */
  }
}