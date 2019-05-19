package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.util.Try
import java.io._;
import scala.collection.mutable.ListBuffer
class Key(var attrsIndex:Map[Int,Any]){

  
  
}

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
  
  def getHashMapValue(curKey :collection.mutable.Map[Int,Any],count : collection.mutable.HashMap[collection.mutable.Map[Int,Any], (Double,Double)], agg:String): (Double,Double) ={
    var gotten = (0.0,0.0);
    if(agg == "COUNT" || agg == "SUM" ||agg == "AVG")gotten = count.getOrElse(curKey,(0.0,0.0));
    else if(agg == "MIN") gotten =  count.getOrElse(curKey,(Double.MaxValue,0.0)); // keep max value possible as initial state for finding min value in dataset
    else gotten =  count.getOrElse(curKey,(Double.MinValue,0.0));
    
    
  return gotten;
    }
  
  def accumulateFunc(x:(Double,Double) , y:(Double,Double), agg:String): (Double,Double) ={
    var value  = 0.0;
    if(agg == "COUNT" || agg == "SUM" || agg == "AVG")value = x._1+y._1;
    else if (agg == "MIN") value = math.max(x._1,y._1);
    else if (agg == "MAX") value =math.max(x._1,y._1);
    return (value,x._2+y._2);
  }
  
  def genPartialCells(curKey :collection.mutable.Map[Int,Any],curValue:Double,curValue2:Double):ListBuffer[(collection.mutable.Map[Int,Any],(Double,Double))]={
    var curList = new ListBuffer[(collection.mutable.Map[Int,Any],(Double,Double))]();
    var colsList = curKey.keySet.toList
    for (i<- 0 to colsList.size + 1 ){
      val this_it = colsList.combinations(i);
      while(this_it.hasNext){
        var thisSet = this_it.next().toSet;
        var keyMapFiltered = collection.mutable.Map[Int,Any]();
        thisSet.foreach( x=> keyMapFiltered +=  ((x)-> curKey(x)));
        curList += ((keyMapFiltered,(curValue,curValue2))); //immutable map
        
      }
      
    }
    return curList;
  }
  def makeStringKey( x:(collection.mutable.Map[Int,Any],(Double,Double)),indices:List[Int],agg:String):(String,Double)={
    var key_str:String = "";
    var count:Int = 0;
    for (i <- indices){
      if(count>0)key_str += " , "
      if( x._1.contains(i)){
        key_str += x._1(i)  
      }
      else{
        key_str +="*"
      }
     count += 1;
    }
    var result:Double = x._2._1;
    if(agg == "AVG")
    {
      var c = x._2._2
      if(c == 0) c = c+1;
        result = result/ c;
    }
    return (key_str,result)
  }
}

object MyFunctionsNaive {
  
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
  
  def accumulateFunc(x:(Double,Double) , y:(Double,Double), agg:String): (Double,Double) ={
    var value  = 0.0;
    if(agg == "COUNT" || agg == "SUM" || agg == "AVG")value = x._1+y._1;
    else if (agg == "MIN") value = math.max(x._1,y._1);
    else if (agg == "MAX") value =math.max(x._1,y._1);
    return (value,x._2+y._2);
  }
  
  def genRegionTupes(listRegions:ListBuffer[List[Int]] , currentRow:Row,index: Int, agg: String): ListBuffer[(collection.mutable.Map[Int,Any],(Double,Double))] = {
    var thisList = new ListBuffer[(collection.mutable.Map[Int,Any],(Double,Double))];
    for (region <- listRegions){
      var thisMap = MyFunctionsNaive.genMap(region,currentRow)
      thisList += ((thisMap,(MyFunctionsNaive.genValue(index,currentRow,agg),1.0)));
    }
    thisList
    
   
  }

}


class CubeOperator(reducers: Int) {
   
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
    val t1 = System.currentTimeMillis()

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()
    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    val mrspreadMap = rdd.map(x=>(MyFunctions.genMap(index,x),(MyFunctions.genValue(indexAgg,x,agg),1.0)));

  val mrspreadReduce = mrspreadMap.reduceByKey((x,y)=>MyFunctions.accumulateFunc(x,y,agg),reducers).persist()
  
  val mrspreadPartialCells = mrspreadReduce.flatMap((x)=>MyFunctions.genPartialCells(x._1,x._2._1,x._2._2));
  
  val mrAssembleReduce = mrspreadPartialCells.reduceByKey((x,y)=>MyFunctions.accumulateFunc(x,y,agg),reducers)
  
  val stringKey = mrAssembleReduce.map((x)=>MyFunctions.makeStringKey(x,index,agg))
  
  val sorted = stringKey.sortByKey();

  val duration = (System.currentTimeMillis() - t1) / 1000.0

  sorted
  
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
   val t1 = System.currentTimeMillis()
    var rdd = dataset.getRDD()
    rdd = rdd.repartition(reducers)
    val schema = dataset.getSchema()

    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    
    var allPartitions = collection.mutable.ListBuffer[List[Int]]()
    allPartitions
    for(x<- Range(0,index.size+1)){
      for (l <- index.combinations(x).toList)
          allPartitions += l
    }
    
    val afterMap = rdd.flatMap(x=> (MyFunctionsNaive.genRegionTupes(allPartitions,x,indexAgg, agg)));
    
    val reduceRegions = afterMap.reduceByKey((x,y)=> {MyFunctionsNaive.accumulateFunc(x,y,agg)},reducers);
    
    val stringKey = reduceRegions.map((x)=>MyFunctions.makeStringKey(x,index,agg))
    
    val sorted = stringKey.sortByKey();

    val duration = (System.currentTimeMillis() - t1) / 1000.0

    sorted
  }

}
