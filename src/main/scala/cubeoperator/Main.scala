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
    // test different input sizes with reducers = 16
    val reducers = 16

    //val inputFile= "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\CS422-Project2-Private\\src\\main\\resources\\lineorder_small.tbl"
    val inputFile0= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_small_half.tbl"
    val inputFile1= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_small.tbl"
    
    val inputFile2= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_medium_half.tbl"
    val inputFile3= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_medium.tbl"
    
    val inputFile4= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_big_half.tbl"
    val inputFile5= "/Users/joseph/Desktop/CS422-Project2-Private/src/main/resources/lineorder_big.tbl"
    
    
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
    .load(inputFile0)
      
    val rdd = df.rdd
    val rdd_row = rdd.take(1) 
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(rdd_row)
    print("size is ")
    println(stream.size)
     

    /*
    val nh = 1;
    val schema = df.schema.toList.map(x => x.name)
    var groupingAttributes = List("lo_suppkey","lo_shipmode")
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
    val sc = SparkContext.getOrCreate()
    val session = SparkSession.builder().getOrCreate();
    val df_sub = session.createDataFrame(subsam.map(_._2), df.schema)
    //df_sub.show()
     */
    
    //println("---RDD ---");
    //rdd.take(1000).map(println );
    //println("---RDD done ---");
    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    // Local schema lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey, lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice, lo_ordertotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax, lo_commitdate, lo_shipmode)
    // Local test
    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")
    // Cluster test
    //var groupingList = List("l_suppkey","l_shipmode","l_shipdate")
    println("fast")
    val res = cb.cube(dataset, groupingList, "lo_quantity", "AVG")
    println("naive")
    val res2 = cb.cube_naive(dataset, groupingList, "lo_quantity", "AVG")

    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */


    //Perform the same query using SparkSQL
        val tsql = System.currentTimeMillis()
        val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
          .agg(sum("lo_supplycost") as "avg supplycost");
        q1.show
        val duration = (System.currentTimeMillis() - tsql) / 1000.0
        print("duration of SparkSQL cube is: ")
        println(duration)
       //println("cube here");

  }
}