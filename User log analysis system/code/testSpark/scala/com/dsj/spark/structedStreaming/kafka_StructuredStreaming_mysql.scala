package com.dsj.spark.structedStreaming
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
object kafka_StructuredStreaming_mysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                  .builder
                  .appName("sogoulogs")
                  .master("local[2]")
                  .getOrCreate()
  
    import spark.implicits._
    //读取数据
     val df = spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
              .option("subscribe", "sogoulogs")
              .load()
      val ds = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
      
    val filter = ds.map(x =>x._2).map(_.split(",")).filter(_.length==6)
    
    //统计所有新闻话题浏览量
    val newsCounts = filter.map(x =>x(2)).groupBy("value").count().toDF("name","count")
    
    //输出数据
    val writer = new JDBCSink(Constants.url,Constants.userName,Constants.passWord)
    val query = newsCounts.writeStream
                        .foreach(writer)
                        .outputMode("update")
                        .trigger(Trigger.ProcessingTime("2 seconds"))
                        .start()
    //统计每个时段新闻浏览量                    
    val periodCounts = filter.map(x =>x(0)).groupBy("value").count().toDF("logtime","count")
    val writer2 = new JDBCSink2(Constants.url,Constants.userName,Constants.passWord)
    val query2 = periodCounts.writeStream
                        .foreach(writer2)
                        .outputMode("update")
                        .trigger(Trigger.ProcessingTime("2 seconds"))
                        .start()
    
    query.awaitTermination()
    query2.awaitTermination()
  }
}