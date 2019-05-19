package sampling
import scala.util.control.Breaks._
import java.lang.instrument.Instrumentation;
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try
import java.io.{ByteArrayOutputStream, ObjectOutputStream,PrintWriter, File}
object MyFunctions {
  
  def parseDouble(s: Any): Option[Double] = Try { s.toString.toDouble }.toOption
  
  def genMap(indicesToKeep:List[Int],currentRow:Row): collection.mutable.Map[Int,Any] = {
    var mapKey = collection.mutable.Map[Int,Any]();
    indicesToKeep.foreach( x=> mapKey +=  (x-> currentRow.get(x)));
    return mapKey
  }
  def genValue(index:Int , currentRow:Row,agg: String): Double ={
    var value = 0.0;
    if(agg == "COUNT")value = 1.0;
    else value = MyFunctions.parseDouble(currentRow.get(index)).getOrElse(0.0);
    return value
  }
}

object SubSampler{
  def subSample(df: DataFrame, groupingAttributes:List[String] , aggAttribute: String,nRows:Int,ci: Double):(DataFrame,Double,Double,Int,Int,scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double])={
    val schema = df.schema.toList.map(x => x.name)
    var rdd = df.rdd
    println("Schema is below")
    println(schema)
    println(groupingAttributes)
    val index = groupingAttributes.map(x => schema.indexOf(x))
    println(index)
    val indexAgg = schema.indexOf(aggAttribute)
    
    val keyedRow = rdd.map(x=>(MyFunctions.genMap(index,x),x));
    val keyed = rdd.map(x=>{
      val res = MyFunctions.genValue(indexAgg,x,"SUM");
      (MyFunctions.genMap(index,x),(1,res,res*res))
    })
    val count_sums_sumsqr = keyed.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,x._3+y._3))
    val counts_mean_std = count_sums_sumsqr.map ( x => {
      val mean = x._2._2/x._2._1 ; //div sums / count to get mean
      val mean_sq = x._2._3/x._2._1;
      (x._1,(x._2._1,mean , Math.sqrt(mean_sq - mean * mean)*x._2._1)) // key , (count, mean , std*count) 
      });
    val sigma_std_mult_count= counts_mean_std.reduce((x,y)=> (x._1,(1,1.0,x._2._3 + y._2._3))); // calc sum *count of std*count across stratum
    //println(sigma_std_mult_count)

    val num_unique_keys = counts_mean_std.map(x=>(x._1,1)).reduce((x,y)=> (x._1,x._2+y._2));
    println("num unique keys")
    println(num_unique_keys)
    
    val nRowsFinal = Math.ceil(Math.max(num_unique_keys._2,nRows*2)) // make sure at-least 2 row per sample is kept
    println(nRowsFinal)
    
    val nh = counts_mean_std.map(x=>{
      val nh_this =nRowsFinal * (x._2._3)/sigma_std_mult_count._2._3;
      (x._1,(x._2._1,x._2._2 ,nh_this ))
    })  // formula for nh from https://stattrek.com/survey-research/stratified-sampling-analysis.aspx?tutorial=samp
    
    val counts = nh.map(x=>(x._1,if (x._2._3<x._2._1) x._2._3/x._2._1 else 1.0)); // if nh < Nh then keep nh/Nh else keep all
    
    // To test if sum(nh) = nRows use println(nh.reduce((x,y)=> (x._1,(1,1.0,x._2._3 + y._2._3))))
    //nh.collect(10).map(println)
    //
    
    val res = counts.collectAsMap()
    val subsam = keyedRow.sampleByKey(false,res,seed = 100);
    
    val estimator_rdd = subsam.map(x=> (x._1,(1,MyFunctions.genValue(indexAgg,x._2,"SUM"))))
    val est_count_mean =  estimator_rdd.reduceByKey((x,y)=>{(x._1+y._1,x._2+y._2)}).map(x=>{(x._1,(x._2._1,x._2._2/x._2._1))}); // key,(count,mean)
    val subsam_mean_joined = est_count_mean.join(estimator_rdd);
    val subsam_sigma_mapped = subsam_mean_joined.map(x=>{(x._1,(Math.pow(x._2._2._2 - x._2._1._2,2)/x._2._1._1 ,x._2._1._1))}); // (key,((xih-meanh)^2/nh,nh))
    val subsam_sigma = subsam_sigma_mapped.reduceByKey((x,y)=>{(x._1 + y._1,x._2)});
    val NH = nh.map(x=>{(x._1,x._2._1)}); // key , NH
    val subsam_sigma_NH= subsam_sigma.join(NH).map(x=>{
      val nh_x = x._2._1._2; 
      val sigma_x = x._2._1._1 ; 
      val NH_this = x._2._2; 
      (x._1, (Math.pow(NH_this,2)*(1-nh_x*1.0/NH_this)*sigma_x/nh_x,NH_this));// key, val_to_sum,NH 
    });
    val se_sum = subsam_sigma_NH.reduce((x,y)=>{(x._1,(x._2._1+y._2._1,x._2._2+y._2._2));}) //summed,N
    val se = Math.sqrt(se_sum._2._1)/se_sum._2._2; // se according to formula at acc to formula at https://stattrek.com/survey-research/stratified-sampling-analysis.aspx?tutorial=samp
    val mean_sub = est_count_mean.map((x) => {(x._1,x._2._2)}).join(NH).map(x=>(x._2._1*x._2._2/se_sum._2._2)).reduce(_+_); 
    println("mean is "+ mean_sub);
    subsam_sigma_mapped.take(10).map(println)
    subsam_sigma.take(10).map(println)
    subsam_sigma_NH.take(10).map(println)
    println(se)
    val session = SparkSession.builder().getOrCreate();
    val df_sub = session.createDataFrame(subsam.map(_._2), df.schema)
    var z_map = collection.mutable.Map[Double,Double]();
    z_map+=(0.95->1.96);
    val alpha = 1 - ci/100;
    val criti_prob = 1-alpha/2;
    var z = z_map(0.95)
    var error_fract = -10.0;
    if(Math.abs(mean_sub)>0) error_fract = z*se/mean_sub;
    return (df_sub,error_fract,se,nRowsFinal.toInt,se_sum._2._2,res);
    
  }
}



object Sampler {
  def sample(lineitem: DataFrame, storageBudgetBytes: Long ,e:Double,ci: Double): (List[DataFrame], (List[Int],List[List[String]],List[Boolean],List[scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]])) = {
    // TODO: implement
    val pw = new PrintWriter(new File("./KeshavSampler.txt" ))
    pw.write("sampler is:  ")
   
    val rdd = lineitem.rdd
    val rdd_row = rdd.take(1) 
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(rdd_row)
    var numRowsAllowed = storageBudgetBytes/stream.size
    pw.write("num rowss allowed are:  "+numRowsAllowed.toString)
    val listQueriesSamples = List(1,3,5,7,10)//add 3 too later.
    val groupingAttributesAll = Map(1->List("l_returnflag","l_linestatus","l_shipdate"),3->List("l_orderkey","l_shipdate"),5->List("l_orderkey" , "l_suppkey"),7->List("l_shipdate"),10->List("l_orderkey" , "l_returnflag"),12->List("l_shipmode", "l_commitdate" ,"l_shipdate", "l_receiptdate" , "l_orderkey"),17-> List("l_partkey" , "l_quantity"),18->List("l_orderkey" , "l_quantity"),19->List("l_quantity", "l_shipmode" , "l_shipinstruct"),20->List("l_partkey" ,"l_suppkey" ,"l_shipdate" ))
    val resultAll = collection.mutable.Map[Int,(DataFrame,Double,Double,Int,Int,scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double])]()
    val cantstoreAll = collection.mutable.Map[Int,Boolean]()
    val estAttrAll = Map(1->"l_extendedprice",3->"l_extendedprice")
    var listDF = new ListBuffer[DataFrame]()
    var listGrouping = new ListBuffer[List[String]]()
    var listBooleans = new ListBuffer[Boolean]()
    var listAllCountFracs = new ListBuffer[scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]]()
    var n:Int =  100;
    var cant_satisfy_error = false;
    var zeroMean = false;
    var groupingAttributes = groupingAttributesAll(1)
    var estAttr = estAttrAll(1)
    val session = SparkSession.builder().getOrCreate();
    val df_use = session.createDataFrame(lineitem.rdd.sample(false, 0.15, 100), lineitem.schema)
    for(v <- listQueriesSamples){
      
    pw.write("reading now is:  "+v+"\n") 
    n =  100;
    
    cant_satisfy_error = false;
    zeroMean = false;
    groupingAttributes = groupingAttributesAll(v)
    estAttr = "l_extendedprice"

    var res:(DataFrame,Double,Double,Int,Int,scala.collection.Map[scala.collection.mutable.Map[Int,Any],Double]) = null;
    
    do{
    res = SubSampler.subSample(df_use,groupingAttributes,estAttr,n,ci);
    if(n<=res._4) n = res._4
    n *= 2//Math.log(n).toInt 
    if(res._2<0) { zeroMean = true;}
    
    if(n>res._5 || n > numRowsAllowed) {cant_satisfy_error = true;}
    println(res._2)
    pw.write("error:  "+res._2.toString+"\n") 
    pw.write("n:  "+n.toString+"\n") 
    println(n)
    }while(res._2>=e && res._2>0 && n<res._5 && n < numRowsAllowed);
    println(res._2)
    println(res._4)
    pw.write("done now res is:  "+res._2+","+res._4+"\n") 
    if(!cant_satisfy_error) numRowsAllowed -= res._4; // these many rows are used up
    resultAll += ( v-> res); 
    cantstoreAll += (v->cant_satisfy_error);
    println(cant_satisfy_error)
    listDF += res._1
    listGrouping += groupingAttributes
    listBooleans += cant_satisfy_error
    listAllCountFracs += res._6 
    }
    for ( i<- 1 to 20)
      listBooleans += true
      
    pw.close
    return (listDF.toList,(listQueriesSamples,listGrouping.toList,listBooleans.toList,listAllCountFracs.toList))
  }
}
