package sampling

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import java.io._
import scala.io.Source
import java.time.LocalDate;

object Main {
  def main(args: Array[String]) {
  

    
    println(LocalDate.of(1998,12,1).minusDays(3).toString)


    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[*]")
    sparkConf.set("spark.network.timeout", "10000000s")
    sparkConf.set("spark.executor.heartbeatInterval","1000000s");
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)  
    val session = SparkSession.builder().getOrCreate();

    val inputFileLineItem = "/cs422-data/tpch/sf100/parquet/lineitem.parquet"
    val inputFileNation = "/cs422-data/tpch/sf100/parquet/nation.parquet"
    val inputFileSupplier = "/cs422-data/tpch/sf100/parquet/supplier.parquet"
    val inputFileCustomer = "/cs422-data/tpch/sf100/parquet/customer.parquet"
    val inputFileOrders = "/cs422-data/tpch/sf100/parquet/order.parquet"
    val inputFilepartsupp = "/cs422-data/tpch/sf100/parquet/partsupp.parquet"
    val inputFileRegion = "/cs422-data/tpch/sf100/parquet/region.parquet"
    val inputFilepart = "/cs422-data/tpch/sf100/parquet/part.parquet"
    


    var desc = new Description
    desc.lineitem = sqlContext.read.option("delimiter", "|").parquet(inputFileLineItem);    
    
    desc.customer = sqlContext.read.option("delimiter", "|").parquet(inputFileCustomer);
    desc.orders  = sqlContext.read.option("delimiter", "|").parquet(inputFileOrders);
    
    desc.supplier = sqlContext.read.option("delimiter", "|").parquet(inputFileSupplier);
    desc.nation = sqlContext.read.option("delimiter", "|").parquet(inputFileNation);
    desc.part = sqlContext.read.option("delimiter", "|").parquet(inputFilepart);
    desc.partsupp = sqlContext.read.option("delimiter", "|").parquet(inputFilepartsupp);
    desc.region = sqlContext.read.option("delimiter", "|").parquet(inputFileRegion);
    

    desc.e = 0.1
    desc.ci = 0.95
    
    val tmp = Sampler.sample(desc.lineitem, 100000000, desc.e, desc.ci)
    desc.samples = tmp._1
    desc.sampleDescription = tmp._2

    Executor.execute_Q1(desc, sqlContext,session, List("3 months"))
    Executor.execute_Q3(desc, sqlContext,session, List("BUILDING","1995-03-15"))
    Executor.execute_Q5(desc, sqlContext,session, List("ASIA","1994-01-01"))
    
    Executor.execute_Q10(desc, sqlContext,session, List("1993-10-01"))
    Executor.execute_Q19(desc, sqlContext,session, List("Brand#12","Brand#23","Brand#34","1","10","20"))
    Executor.execute_Q20(desc, sqlContext,session, List("forest","1994-01-01","CANADA"))
    Executor.execute_Q19(desc, sqlContext,session, List("Brand#12","Brand#23","Brand#34","1","10","20"))
    Executor.execute_Q18(desc, sqlContext,session, List("300"))
    Executor.execute_Q17(desc, sqlContext,session, List("Brand#23","MED BOX"))
    Executor.execute_Q12(desc, sqlContext,session, List("MAIL", "SHIP","1994-01-01"))
    Executor.execute_Q10(desc, sqlContext,session, List("1993-10-01"))
    Executor.execute_Q5(desc, sqlContext,session, List("ASIA","1994-01-01"))
    Executor.execute_Q6(desc, sqlContext,session, List("1994-01-01","0.06","24"))
    Executor.execute_Q7(desc, sqlContext,session, List("FRANCE","GERMANY"))
    Executor.execute_Q9(desc, sqlContext,session, List("green"))
    
        //val rdd = RandomRDDs.uniformRDD(sc, 100000)
    /*
    val inputFileLineItem = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\lineitem.parquet"
    val inputFileCustomer = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\customer.parquet"
    val inputFileOrders = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\order.parquet"
    val inputFileNation = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\nation.parquet"
    val inputFileSupplier = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\supplier.parquet"
    val inputFilepartsupp = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\partsupp.parquet"
    val inputFileRegion = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\region.parquet"
    val inputFilepart = "C:\\Users\\Dell\\Documents\\courses\\2019\\semA\\DB\\big\\tpch_parquet_sf1\\part.parquet"
     */ 
    


  }     
}
