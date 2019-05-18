package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import java.io._
import scala.io.Source

object Main {
  def main(args: Array[String]) {
  
    //val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    //val sc = SparkContext.getOrCreate()
    // PrintWriter
    
    //val qs = new GQueries 
    //val s = qs.q1
    //var params = List("3 months")
    //var query = ExecutorHelpers.multipleReplace(s, params)

    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[*]")
    //sparkConf.set("spark.network.timeout", "3600s")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)  
    val session = SparkSession.builder().getOrCreate();
    //val rdd = RandomRDDs.uniformRDD(sc, 100000)

    val inputFileLineItem = "/cs422-data/tpch/sf100/parquet/lineitem.parquet"

    val inputFileNation = "/cs422-data/tpch/sf100/parquet/nation.parquet"
    val inputFileSupplier = "/cs422-data/tpch/sf100/parquet/supplier.parquet"
    val inputFileCustomer = "/cs422-data/tpch/sf100/parquet/customer.parquet"
    val inputFileOrders = "/cs422-data/tpch/sf100/parquet/order.parquet"
    val inputFilepartsupp = "/cs422-data/tpch/sf100/parquet/partsupp.parquet"
    val inputFileParts = "/cs422-data/tpch/sf100/parquet/parts.parquet"
    
    val dfLineItem = sqlContext.read.option("delimiter", "|").parquet(inputFileLineItem);    
    val dfSupplier = sqlContext.read.option("delimiter", "|").parquet(inputFileSupplier);
    val dfNation = sqlContext.read.option("delimiter", "|").parquet(inputFileNation);
    val dfPartsupp = sqlContext.read.option("delimiter", "|").parquet(inputFilepartsupp);
    val dfOrders = sqlContext.read.option("delimiter", "|").parquet(inputFileOrders);
    val dfCustomer = sqlContext.read.option("delimiter", "|").parquet(inputFileCustomer);

    

    var desc = new Description
    desc.lineitem = dfLineItem
    desc.customer = sqlContext.read.option("delimiter", "|").parquet(inputFileCustomer);
    desc.orders  = sqlContext.read.option("delimiter", "|").parquet(inputFileOrders);
    desc.supplier = sqlContext.read.option("delimiter", "|").parquet(inputFileSupplier);
    desc.nation = sqlContext.read.option("delimiter", "|").parquet(inputFileNation);
    desc.region = sqlContext.read.option("delimiter", "|").parquet(inputFilepartsupp);
    
    desc.e = 0.1
    desc.ci = 0.95

    val tmp = Sampler.sample(desc.lineitem, 1000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    // check storage usage for samples


    Executor.execute_Q1(desc, sqlContext,session, List("3 months"))

     //val inputFile= "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\CS422-Project2-Private\\src\\main\\resources\\lineorder_small.tbl"
     //val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    //val ctx = new SparkContext(sparkConf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
   //val df = sqlContext.read.option("delimiter", "|").parquet(inputFile);
  
      
     //val rdd = df.rdd
     //val tmp = Sampler.sample(df, 1000000, 0.003, 0.95)
     /*
    val query = "SELECT * FROM nation"
    var s = ""
    val bufferedSource = Source.fromFile("/home/ksingh/3_modify.sql")
    for (line <- bufferedSource.getLines) {
      s += line + "  "
      println(line.toUpperCase)
      }
    bufferedSource.close
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)
    

    
    dfPartsupp.show()
    dfNation.show()
    dfSupplier.show()
    
    dfNation.createOrReplaceTempView("nation");
    dfPartsupp.createOrReplaceTempView("partsupp");
    dfSupplier.createOrReplaceTempView("supplier");
    dfOrders.createOrReplaceTempView("orders");
    dfCustomer.createOrReplaceTempView("customer");
    dfLineItem.createOrReplaceTempView("lineitem");
    val sqlDF = sqlContext.sql(s);
    sqlDF.show();
    * 
    */
  }     
}
