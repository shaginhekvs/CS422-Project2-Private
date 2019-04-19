package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import scala.util.Try
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
    return value;
  }
  
  def getHashMapValue(curKey :collection.mutable.Map[Int,Any],count : collection.mutable.HashMap[collection.mutable.Map[Int,Any], Double], agg:String): Double ={
    var gotten = 0.0;
    if(agg == "COUNT" || agg == "SUM" ||agg == "AVG")gotten = count.getOrElse(curKey,0.0);
    else if(agg == "MIN") gotten =  count.getOrElse(curKey,Double.MaxValue); // keep max value possible as initial state for finding min value in dataset
    else gotten =  count.getOrElse(curKey,Double.MinValue);
    
  return gotten;
    }
  
  def accumulateFunc(x:Double , y:Double, agg:String): Double ={
    var value  = 0.0;
    if(agg == "COUNT" || agg == "SUM" || agg == "AVG")value = x+y;
    else if (agg == "MIN") value = math.max(x,y);
    else if (agg == "MAX") value =math.max(x,y);
    return value;
  }
  
  def genPartialCells(curKey :collection.mutable.Map[Int,Any],curValue:Double):ListBuffer[(collection.mutable.Map[Int,Any],Double)]={
    var curList = new ListBuffer[(collection.mutable.Map[Int,Any],Double)]();
    var colsList = curKey.keySet.toList
    for (i<- 1 to colsList.size){
      val this_it = colsList.combinations(i);
      while(this_it.hasNext){
        var thisSet = this_it.next().toSet;
        var keyMapFiltered = collection.mutable.Map[Int,Any]();
        thisSet.foreach( x=> keyMapFiltered +=  ((x)-> curKey(x)));
        curList += ((keyMapFiltered,curValue)); //immutable map
      }
      
    }
    return curList;
  }
}



class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  
 
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()
    rdd.take(10).map( x=> println(x.get(5)) );
    
    println(schema)
    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    val mrspreadMap = rdd.map(x=>(MyFunctions.genMap(index,x),MyFunctions.genValue(indexAgg,x,agg)));
    mrspreadMap.take(10).map(println );
    val mrspreadCombine =  mrspreadMap.mapPartitions(it =>
    it.foldLeft(new collection.mutable.HashMap[collection.mutable.Map[Int,Any], Double])(
      (count, row) => count += (row._1 -> MyFunctions.accumulateFunc(MyFunctions.getHashMapValue(row._1,count ,agg ),row._2,agg))
    ).toIterator
  )
  println("mrspread combine below")
  mrspreadCombine.take(10).map(println );
  
  val mrspreadReduce = mrspreadCombine.reduceByKey((x,y)=>MyFunctions.accumulateFunc(x,y,agg)).persist()
  println("mrspread reduce below")
  mrspreadReduce.take(10).map(println );  
  val mrspreadPartialCells = mrspreadReduce.flatMap((x)=>MyFunctions.genPartialCells(x._1,x._2));
  println("mrspread partial cells below")
  mrspreadPartialCells.take(10).map(println );  
  null
    
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation
    null
  }

}
