package com.dsj.spark.sql
import org.apache.spark.sql._

object rddDataFrameDataSet {
  def main(args: Array[String]): Unit = {
    
    
     val spark = SparkSession
      .builder()
      .appName("topn")
      .master("local[2]")
      .getOrCreate()
//      
//    val df = rddToDataFrame(spark)
//    df.printSchema()
//    df.select("_1","_2").show()
//  
//      val ds = rddToDataSet(spark)
//      ds.createTempView("wordcount")
//      spark.sql("select word,count(word) as count from wordcount group by word order by count desc limit 5").show()
//      
      val df = dataSetToDataFrame(spark)
      df.select("word", "count").groupBy("word").count().show()
   
//      dataSetToRDD(spark)
      
//        dataFrameToRDD(spark)
  }
  /**
   * DataFrame 转 RDD
   */
  def dataFrameToRDD(spark:SparkSession) = {
     // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val df = spark.read.json("C:\\Users\\dell\\网课\\26Spark2.3.x SQL 离线计算\\数据集\\people.json")
    df.printSchema()
    df.select("name").show()
    
    df.createTempView("people")
    val result = spark.sql("select name from people where age >=13 and age <=19")
    result.rdd.foreach(println)  
  }
  
  
  
  /**
   * DataSet 转 RDD
   */
  def dataSetToRDD(spark:SparkSession) = {
    val rdd = spark.read
                   .textFile("D:\\软件\\百度网盘下载地址\\数据集\\wc.txt")
                   .rdd
                   .flatMap(_.split("\\s+"))
                   .map((_,1))
                   .reduceByKey(_+_)
                   .map(x =>(x._2,x._1))
                   .sortByKey(false)
                   .map(x =>(x._2,x._1))
                   
    rdd.take(3).foreach(println)
  }
  
  
  
  /**
   * DataSet 转 DataFrame
   */
  def dataSetToDataFrame(spark:SparkSession):DataFrame = {
     // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val ds = spark.read
                  .textFile("D:\\软件\\百度网盘下载地址\\数据集\\wc.txt")
                  .flatMap(_.split("\\s+"))
                  .map((_,1))
    val df = ds.toDF("word","count")
    df
    
  }
  
  
  /**
   * rdd 转 DataSet
   */
  case class wordcount(word:String,count:Int)
  def rddToDataSet(spark:SparkSession):Dataset[wordcount] = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    
    val ds = spark.sparkContext.textFile("D:\\软件\\百度网盘下载地址\\数据集\\wc.txt")
                               .flatMap(_.split("\\s+"))
                               .map((_,1))
                               .map(x =>(wordcount(x._1,x._2)))
                               .toDS()
    ds
  }
  
  
  
  /**
 * rdd 转 DataFrame 
 */
  def rddToDataFrame(spark:SparkSession):DataFrame = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    
    val fileRDD = spark.sparkContext.textFile("D:\\软件\\百度网盘下载地址\\数据集\\wc.txt")
    
    val wordCountRDD = fileRDD.flatMap(_.split("\\s+"))
                              .map((_,1))
                              .reduceByKey(_+_)
                              .map(x =>(x._2,x._1))
                              .sortByKey(false)
                              .map(x =>(x._2,x._1))
                    
    val df = wordCountRDD.toDF()
    df
  }
  
}