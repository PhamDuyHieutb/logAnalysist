import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object TestLog{

    def Combine(event1:String,event2: String):String ={
        (event1,event2) match {
            case ("true","true") => event1
            case ("false","false") => event1
            case ("true","false") => ("ci")
            case ("false","true") => ("ci")
            case ("ci",_) => event1
            case (_,"ci") => event2
        }

    }




  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("testlog")
    // your handle to SparkContext to access other context like SQLContext
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

//    val spark = SparkSession.builder().master("local").appName("Log Query").enableHiveSupport().getOrCreate()
    val logdata = sqlContext.read.parquet("/data/Parquet/AdnLog/2017_09_19/{parquet_logfile_at_21h_00.snap,parquet_logfile_at_21h_05.snap,parquet_logfile_at_21h_10.snap,parquet_logfile_at_21h_15.snap,parquet_logfile_at_21h_20.snap,parquet_logfile_at_21h_25.snap}").repartition(10)



    logdata.registerTempTable("log")
//    val stringify = udf((vs: Seq[BigInt]) => vs.mkString(",") )

    val sql= sqlContext.sql("select guid,bannerId,time_group.time_create,click_or_view from log limit 1000")

//    sql.write.format("com.databricks.spark.csv").csv("/home/hadoop/result.csv")
//    val stringify = udf( (time_create:BigInt,cookie_create:BigInt) => castToString(time_create,cookie_create) )
//    val sqlClickResult = sql.map(e => {
//    var v="0"
//    if (e.getBoolean(3)) v ="1"
//  ((e.getLong(0), e.getInt(1),(e.getLong(2)/900000)*900000), (v))
//})
    val sqlClickResult = sql.rdd.filter(e => e.getBoolean(3))
    val sqlImpressionResult = sql.rdd.filter(e => !e.getBoolean(3))


   /* val re = sqlClickResult.reduceByKey((v1,v2) => {
      var a1 = v1.split("_")
      var a2 = v2.split("_")
      (a1(0).toInt+a2(0).toInt) +"_"+(a1(1).toInt+a2(1).toInt)
    }).map((a) =>{
      var p = a._2.split("_")
      var result = (p(0).toInt)*1.0/(p(0).toInt+p(1).toInt)
       (a._1,result)
    } ).saveAsTextFile("/user/hieupd/logAnalysist/part5")
    */


//    val re = sqlImpressionResult.union(sqlClickResult).reduceByKey(Combine).coalesce(1).saveAsTextFile("/user/hieupd/logAnalysist/part1")
    val reClick = sqlImpressionResult.union(sqlClickResult).map(a => ((a.getLong(0),a.getInt(1),(a.getLong(2)/900000)*900000),a.getBoolean(3).toString)).reduceByKey(Combine).map(a => (a._2,1)).reduceByKey((a1,a2)=> a1+a2)
    val dem = reClick.map(a => a._2).sum()
    val ctr = reClick.map(a => (a._1,a._2*1.0000/dem)).repartition(1).saveAsTextFile("/user/hieupd/logAnalysist/part6")
    sql.write.mode(saveMode = "overwrite").format("com.databricks.spark.csv").save("/user/hieupd/logAnalysist/part6/sqlSelect")
  }
}