package sampling

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.io.Source
import org.apache.spark.sql.{Row, SparkSession}
import scala.util.matching.Regex


object ExecutorHelpers {
  
  
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
  
  def processOutput(sqlDF : DataFrame,desc:Description,session:SparkSession,dataIndex :Int): RDD[Row]={
    val schema = sqlDF.schema.toList.map(x => x.name)
    var colToIndex =collection.mutable.Map [String,Int]();
    schema.foreach(x=> {colToIndex += (x-> schema.indexOf(x))})
    val cols_but_qcs = schema diff desc.sampleDescription._2(dataIndex) //(List[Int],List[List[String]],List[Boolean],List[scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]])
    val all_index = schema.map(x =>schema.indexOf(x))
    val index_vals = cols_but_qcs.map(x => schema.indexOf(x))
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
    /*
    var s = ""
    val bufferedSource = Source.fromFile("/home/ksingh/1_modified.sql")
    print(desc.lineitem.schema) 
    
    for (line <- bufferedSource.getLines) {
      s += line + "  "
      println(line.toUpperCase)
      }
    bufferedSource.close
    */
    //val qs = new Queries
    val qs = new GQueries 
    val s = qs.q1
    if(!desc.sampleDescription._3(0)){
      print("using samples")
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    //var sqlDF = sqlContext.sql(s);
    var sqlDF = sqlContext.sql(query);
    sqlDF.show()
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(0)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,0)
      print("done processing output")
    }
    println("result of query is below ")
    
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
    
    val qs = new Queries
    //val qs = new GQueries 
    desc.customer.createOrReplaceTempView("customer");
    desc.orders.createOrReplaceTempView("orders");
    val s: String = qs.q3
    var query = ExecutorHelpers.multipleReplace(s, params)
    if(!desc.sampleDescription._3(1)){
      desc.samples(1).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    var sqlDF = sqlContext.sql(s);
    sqlDF.show()
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(0)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,0)
      print("done processing output")
    }
    println("result of query is below ")
    res.take(10).map(println)
    res
    
    
  }

  def execute_Q5(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
       // define right param. number
    assert(params.size == 2)
    
    ////val qs = new Queries
    val qs = new GQueries
    
    val s = qs.q5
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q6(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 3)
    ////val qs = new Queries
    val qs = new GQueries   
    val s = qs.q6
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

    def execute_Q7(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q7
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q9(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q9
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q10(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q6
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_11(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q11
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q12(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 3)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q12
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q17(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 2)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q17
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q18(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 1)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q18
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q19(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 6)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q19
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query")
    res.take(10).map(println)
    res
  }

  def execute_Q20(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    
    assert(params.size == 3)
    //val qs = new Queries
    val qs = new GQueries  
    val s = qs.q20
    
    var query = ExecutorHelpers.multipleReplace(s, params)
    
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    
    var sqlDF = sqlContext.sql(s);
    var res = sqlDF.rdd
    //var sqlDF = sqlContext.sql(query);
    if(!desc.sampleDescription._3(2)){
      print("processing output")
      res = ExecutorHelpers.processOutput(sqlDF,desc,session,2)
      print("done processing output")
    }
    println("result of query is below ")
    res.take(10).map(println)
    res
  }
}
