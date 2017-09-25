import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._


object TestLog{

    def Combine(event1:(String, Long),event2: (String,Long)):(String,Long) ={
        (event1._1,event2._1) match {
            case ("c","c") => event1
            case ("i","i") => event1
            case ("c","i") => ("ci",event2._2)
            case ("i","c") => ("ci",event1._2)
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
    val logdata = sqlContext.read.parquet("/data/Parquet/AdnLog/2017_09_19/*")



    logdata.registerTempTable("log")
//    val stringify = udf((vs: Seq[BigInt]) => vs.mkString(",") )
    val sqlTrue = sqlContext.sql("select guid,bannerId,time_group.time_create from log where click_or_view = true").rdd
    val sqlFalse= sqlContext.sql("select guid,bannerId,time_group.time_create from log ").rdd
//    sql.write.format("com.databricks.spark.csv").csv("/home/hadoop/result.csv")
//    val stringify = udf( (time_create:BigInt,cookie_create:BigInt) => castToString(time_create,cookie_create) )
    val sqlClickResult = sqlTrue.map(e => ((e.getLong(0), e.getInt(1)), ("c",e.getLong(2))))
    val sqlImpressionResult = sqlFalse.map(e => ((e.getLong(0), e.getInt(1)),("i",e.getLong(2))))


    val a = sqlImpressionResult.union(sqlClickResult).reduceByKey(Combine).coalesce(1).saveAsTextFile("/user/hieupd/logAnalysist/")

  }
}