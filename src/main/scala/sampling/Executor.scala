package sampling

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try
import scala.io.Source
import org.apache.spark.sql.{Row, SparkSession}
object ExecutorHelpers {
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
     listVals += thisRow._2.get(x);
   }
   });
   (thisRow._1,Row(listVals.toSeq))
  }
  
  def processOutput(sqlDF : DataFrame,desc:Description,session:SparkSession,dataIndex :Int): DataFrame={
    val schema = sqlDF.schema.toList.map(x => x.name)
    var colToIndex =collection.mutable.Map [String,Int]();
    schema.foreach(x=> {colToIndex += (x-> schema.indexOf(x))})
    val cols_but_qcs = schema diff desc.sampleDescription._2(dataIndex) //(List[Int],List[List[String]],List[Boolean],List[scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]])
    val all_index = schema.map(x =>schema.indexOf(x))
    val index_vals = cols_but_qcs.map(x => schema.indexOf(x))
    val index_keys = cols_but_qcs.map(x => schema.indexOf(x))
    val sqlrdd = sqlDF.rdd
    val keyedRow = sqlrdd.map(x=>(MyFunctions.genMap(index_keys,x),x));
    //  scale the values now ,
    val scaled = keyedRow.map(x=> ExecutorHelpers.scaleOutput(x,all_index,index_vals,desc.sampleDescription._4(dataIndex)))
    val df_sub = session.createDataFrame(scaled.map(_._2), desc.lineitem.schema)
    df_sub
  }
}

object Executor {
  
  
  def execute_Q1(desc: Description, sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    // TODO: implement
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
    val qs = new Queries 
    val s = qs.q1
    if(!desc.sampleDescription._3(0)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    var sqlDF = sqlContext.sql(s);
    if(!desc.sampleDescription._3(0)){
      sqlDF = ExecutorHelpers.processOutput(sqlDF,desc,session,0)
    }
    println("result of query")
    sqlDF.show()
    
    //sqlDF.write.csv("/home/ksingh/1_result.csv")
  }

  def execute_Q3(desc: Description,  sqlContext : org.apache.spark.sql.SQLContext,session: SparkSession, params: List[Any]) = {
    // TODO: implement
    assert(params.size == 2)
    // https://github.com/electrum/tpch-dbgen/blob/master/queries/3.sql
    // using:
    // params(0) as :1
    // params(1) as :2
    val qs = new Queries 
    val s = qs.q3
    if(!desc.sampleDescription._3(1)){
      desc.samples(0).createOrReplaceTempView("lineitem");
    }
    else{
      desc.lineitem.createOrReplaceTempView("lineitem");
    }
    var sqlDF = sqlContext.sql(s);
    if(!desc.sampleDescription._3(1)){
      sqlDF = ExecutorHelpers.processOutput(sqlDF,desc,session,1)
    }
    println("result of query 3")
    sqlDF.show()
    
    
  }

  def execute_Q5(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q6(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q7(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q9(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q10(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q11(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q12(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q17(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q18(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q19(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }

  def execute_Q20(desc: Description, session: SparkSession, params: List[Any]) = {
    // TODO: implement
  }
}
