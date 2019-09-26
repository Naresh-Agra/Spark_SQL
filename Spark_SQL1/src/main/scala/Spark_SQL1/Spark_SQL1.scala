package Spark_SQL1

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark_SQL1 {

  def main(args:Array[String]):Unit={
    val conf=new SparkConf().setAppName("Spark_SQL_1").setMaster("local[*]")

    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark=SparkSession
    .builder()
    .config(conf)
    .getOrCreate()
    
    import spark.implicits
    
    val datadf=spark.read.option("delimeter",",").csv("file:///E://Workouts//Data//sample_txns.txt")
    datadf.show(false)
     
    
    }
  
}