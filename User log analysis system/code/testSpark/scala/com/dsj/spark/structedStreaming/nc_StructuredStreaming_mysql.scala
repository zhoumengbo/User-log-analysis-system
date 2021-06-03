package com.dsj.spark.structedStreaming
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object nc_StructuredStreaming_mysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                  .builder
                  .appName("sogoulogs")
                  .master("local[2]")
                  .getOrCreate()
  
    import spark.implicits._
    //读取数据
    val lines = spark.readStream
      .format("socket")
      .option("host", "hadoop03")
      .option("port", 9999)
      .load()
      
    val filter = lines.as[String].map(_.split(",")).filter(_.length==6)
    
    //统计所有新闻话题浏览量
    val newsCounts = filter.map(x =>x(2)).groupBy("value").count().toDF("name","count")
    
    //输出数据
    val writer = new JDBCSink(Constants.url,Constants.userName,Constants.passWord)
    val query = newsCounts.writeStream
                        .foreach(writer)
                        .outputMode("update")
                        .trigger(Trigger.ProcessingTime("2 seconds"))
                        .start()
    query.awaitTermination()
  }
}