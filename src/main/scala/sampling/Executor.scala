package sampling

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate;
import scala.util.Try
import scala.io.Source
import org.apache.spark.sql.{Row, SparkSession}
import scala.util.matching.Regex

import scala.util.Try

object ExecutorHelpers {
  
  def parseInt(s: Any): Option[Int] = Try { s.toString.toInt }.toOption
  
  def multipleReplace(text: String, params: List[Any]): String ={
    var modtext: String = text 

    println(params.size)
    for (i<- 1 to params.size){
      var ch: String = ":" + (i).toString();
      modtext = modtext.replaceAll(ch, params(i-1).toString())

    }
    return modtext
  }
  
  def scaleOutput(thisRow:(collection.mutable.Map[Int,Any],Row),all_index:List[Int],index_vals:List[Int] , NHMap:scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]):(collection.mutable.Map[Int,Any],Row) = {
   var scale_const = 1.0;
   NHMap.keySet.foreach(x=>{
     if(x == thisRow._1){
       scale_const = NHMap(x);
     }
   })
   var listVals = collection.mutable.ListBuffer[Any]();
   all_index.foreach(x=> {if(index_vals contains x){
    listVals += MyFunctions.parseDouble(thisRow._2.get(x)).getOrElse(0.0)/scale_const;
   }
   else{
     listVals += thisRow._2.getAs(x);
   }
   });
   (thisRow._1,Row.fromSeq(listVals.toSeq))
  }
  
  def processOutput(sqlDF : DataFrame,desc:Description,session:SparkSession,dataIndex :Int,index_vals:List[Int]): RDD[Row]={
    val schema = sqlDF.schema.toList.map(x => x.name)
    var colToIndex =collection.mutable.Map [String,Int]();
    schema.foreach(x=> {colToIndex += (x-> schema.indexOf(x))})
    val cols_but_qcs = schema diff desc.sampleDescription._2(dataIndex) //(List[Int],List[List[String]],List[Boolean],List[scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]])
    val all_index = schema.map(x =>schema.indexOf(x))
    val index_keys = cols_but_qcs.map(x => schema.indexOf(x))
    val sqlrdd = sqlDF.rdd
    //println("old")
    //sqlrdd.take(10).map(println)
    val keyedRow = sqlrdd.map(x=>(MyFunctions.genMap(index_keys,x),x));
    //  scale the values now ,
    val NHMap = desc.sampleDescription._4(dataIndex)
    val newMap = collection.mutable.Map[scala.collection.mutable.Map[Int,Any],Double] ();
    NHMap.keySet.foreach(x=>{newMap += (x-> NHMap(x))});
    val scaled = keyedRow.map(x=> ExecutorHelpers.scaleOutput(x,all_index,index_vals,newMap))

    //println("new")
    //scaled.map(_._2).take(10).map(println)
    //val df_sub = session.createDataFrame(scaled.map(_._2), sqlDF.schema)
    scaled.map(_._2)
  }
}

object Executor {
  
  
  def execute_Q1(desc: Description, sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    // For example, for Q1, params(0) is the interval from the where close
    
    val qs = new GQueries 
    val s = qs.q1
    if(!desc.sampleDescription._3(0)){
      print("using samples")
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    var paramsNew = ListBuffer[String]();
    val date = ExecutorHelpers.parseInt(params(0)).getOrElse(3)
    paramsNew += LocalDate.of(1998,12,1).minusDays(date).toString
    var query = ExecutorHelpers.multipleReplace(s, paramsNew.toList)
    var list_cols_scale = List(2,3,4,5,6,7,8,9)
    //var sqlDF = sqlContext.sql(s);
    var sqlDF = sqlContext.sql(query);
    sqlDF.show()
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 1is below ")
    
    res.take(10).map(println)
    res
    
    //sqlDF.write.csv("/home/ksingh/1_result.csv")
  }

  def execute_Q3(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
    
    val qs = new GQueries 
    desc.customer.createOrReplaceTempView("customer");
    desc.orders.createOrReplaceTempView("orders");
    val s: String = qs.q3
    
    
    var query = ExecutorHelpers.multipleReplace(s, params)

    if(!desc.sampleDescription._3(1)){ // index of query
      desc.samples(1).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(query);
    sqlDF.show()
    var res = sqlDF.rdd
    var list_cols_scale = List(1)

    println("result of query 3 is below ")
    res.take(10).map(println)
    res
    
    
  }

  def execute_Q5(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    assert(params.size == 2)
    
    ////val qs = new Queries
    val qs = new GQueries
    
    val s = qs.q5
    var paramsNew = ListBuffer[String]();
    var paramsDate = params(1).toString
    paramsNew += params(0).toString
    paramsNew += params(1).toString
    paramsNew += LocalDate.parse(paramsDate).plusYears(1).toString
    var query = ExecutorHelpers.multipleReplace(s, paramsNew.toList)

    desc.customer.createOrReplaceTempView("customer");
    desc.orders.createOrReplaceTempView("orders");
    desc.supplier.createOrReplaceTempView("supplier");
    desc.nation.createOrReplaceTempView("nation");
    desc.region.createOrReplaceTempView("region");
    desc.lineitem.createOrReplaceTempView("lineitem");
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 7 ")
    res.take(10).map(println)
    /*
    
    if(!desc.sampleDescription._3(2)){
      desc.samples(2).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    

    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query")
    res.take(10).map(println)
    * 
    */
    res
  }

  def execute_Q6(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 3)
    ////val qs = new Queries
    val qs = new GQueries   
    val s = qs.q6
    var paramsNew = ListBuffer[String]();
    var paramsDate = params(0).toString
    paramsNew += params(0).toString
    paramsNew += params(1).toString
    paramsNew += params(2).toString
    paramsNew += LocalDate.parse(paramsDate).plusYears(1).toString
    var query = ExecutorHelpers.multipleReplace(s, paramsNew.toList)

    if(!desc.sampleDescription._3(10)){
      desc.samples(3).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    println("result of query 5")
    res.take(10).map(println)

    res
  }
  

  
   def execute_Q7(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q7
    
    var query = ExecutorHelpers.multipleReplace(s, params)

    desc.customer.createOrReplaceTempView("customer");
    desc.orders.createOrReplaceTempView("orders");
    desc.supplier.createOrReplaceTempView("supplier");
    desc.nation.createOrReplaceTempView("nation");
    
    if(!desc.sampleDescription._3(3)){
      desc.samples(3).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    

    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 7 ")
    res.take(10).map(println)

    res
  
   }

def execute_Q9(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q9
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    desc.orders.createOrReplaceTempView("orders");
    desc.supplier.createOrReplaceTempView("supplier");
    desc.nation.createOrReplaceTempView("nation");
    desc.part.createOrReplaceTempView("part");
    desc.partsupp.createOrReplaceTempView("partsupp");
    
    if(!desc.sampleDescription._3(10)){
      desc.samples(10).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 9")
    res.take(10).map(println)
    res
  }

  def execute_Q10(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q10
    var paramsNew = ListBuffer[String]();
    var paramsDate = params(0).toString
    paramsNew += params(0).toString
    paramsNew += LocalDate.parse(paramsDate).plusYears(1).toString
    println(paramsNew)
    var query = ExecutorHelpers.multipleReplace(s, paramsNew.toList)
    
    desc.customer.createOrReplaceTempView("customer");
    desc.orders.createOrReplaceTempView("orders");
    desc.nation.createOrReplaceTempView("nation");


    
    if(!desc.sampleDescription._3(4)){
      desc.samples(4).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 10")
    res.take(10).map(println)
    res
  }

  def execute_11(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q11
    
    //desc.partsupp.createOrReplaceTempView("partsupp");
    desc.supplier.createOrReplaceTempView("supplier");
    desc.nation.createOrReplaceTempView("nation");
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    

    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd

    println("result of query 11")
    res.take(10).map(println)
    res
  }

  def execute_Q12(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 3)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q12
    var paramsNew = ListBuffer[String]();
    var paramsDate = params(2).toString
    paramsNew += params(0).toString
    paramsNew += params(1).toString
    paramsNew += params(2).toString
    paramsNew += LocalDate.parse(paramsDate).plusYears(1).toString
    var query = ExecutorHelpers.multipleReplace(s, paramsNew.toList)
    
    desc.orders.createOrReplaceTempView("orders");

    
    if(!desc.sampleDescription._3(8)){
      desc.samples(8).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    


    
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd

    println("result of query 12")
    res.take(10).map(println)

    res
  }

  def execute_Q17(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q17
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    desc.part.createOrReplaceTempView("part");

    if(!desc.sampleDescription._3(9)){
      desc.samples(9).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    

    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);

    println("result of query 17")
    res.take(10).map(println)

    res
  }

  def execute_Q18(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q18
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    desc.customer.createOrReplaceTempView("customer");
    desc.orders.createOrReplaceTempView("orders");
    
    if(!desc.sampleDescription._3(10)){
      desc.samples(10).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    

    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 18 ")
    res.take(10).map(println)
    res
  }

  def execute_Q19(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 6)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q19
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    desc.part.createOrReplaceTempView("part");

    
    if(!desc.sampleDescription._3(11)){
      desc.samples(11).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    println("result of query 19")
    res.take(10).map(println)
    res
  }

  def execute_Q20(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 3)

    val qs = new GQueries  
    val s = qs.q20
    var paramsNew = ListBuffer[String]();
    var paramsDate = params(1).toString
    paramsNew += params(0).toString
    paramsNew += params(1).toString
    paramsNew += params(2).toString
    paramsNew += LocalDate.parse(paramsDate).plusYears(1).toString
    var query = ExecutorHelpers.multipleReplace(s, paramsNew.toList)
    
    desc.supplier.createOrReplaceTempView("supplier");
    desc.nation.createOrReplaceTempView("nation");
    desc.part.createOrReplaceTempView("part");
    desc.partsupp.createOrReplaceTempView("partsupp");
    
    if(!desc.sampleDescription._3(12)){
      desc.samples(12).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }

    //var sqlDF = sqlContext.sql(s);
    
    var sqlDF = sqlContext.sql(query);
    var res = sqlDF.rdd

    println("result of query 20 is below ")
    res.take(10).map(println)

    res
  }
}
