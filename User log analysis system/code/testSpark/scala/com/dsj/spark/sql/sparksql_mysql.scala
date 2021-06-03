package com.dsj.spark.sql
import org.apache.spark.sql._
import java.sql.Statement
import java.sql.Connection
import java.sql.DriverManager
/**
 * spark sql 与  mysql 集成大数据项目离线分析
 */
object sparksql_mysql {
  case class sogoulogs(logtime:String,uid:String,keywords:String,resultno:String,clickno:String,url:String)
  def main(args: Array[String]): Unit = {
    
     val spark = SparkSession
                          .builder()
                          .appName("sogoulogs")
                          .master("local[2]")
                          .getOrCreate()
      
      // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    //读取元数据
    val fileRDD = spark.sparkContext
                       .textFile("D:\\软件\\百度网盘下载地址\\数据集\\sogoulogs.log")
   
    //rdd 转 DataSet                   
    val ds = fileRDD.map(line => line.split(",")).map(t =>sogoulogs(t(0),t(1),t(2),t(3),t(4),t(5))).toDS()
    ds.createTempView("sogoulogs")
    
    //统计每个新闻浏览量
    val newsCount = spark.sql("select keywords as name,count(keywords) as count from sogoulogs group by keywords")
    newsCount.show()
    newsCount.rdd.foreachPartition(myFun)
    
    //统计每个时段新闻浏览量
    val peiodCount = spark.sql("select logtime,count(logtime) as count from sogoulogs group by logtime")
    peiodCount.show()
    peiodCount.rdd.foreachPartition(myFun2)
  }
  
  
  /**
   * 新闻浏览量输入MySQL
   */
  def myFun(records:Iterator[Row]): Unit ={
    var conn:Connection = null
    var statement:Statement = null
    
    try{
      val url = Constants.url
      val userName:String = Constants.userName
      val passWord:String = Constants.passWord
      
      //connection连接
      conn = DriverManager.getConnection(url,userName,passWord)
      
      records.foreach(t => {
        val name = t.getAs[String]("name").replaceAll("[\\[\\]]","")
        val count = t.getAs[Long]("count").asInstanceOf[Int]
        
        val sql = "select 1 from newscount "+" where name = '"+name+"'"
        
        val updateSql = "update newscount set count = count+"+count+" where name ='"+name+"'"
        
        val insertSql = "insert into newscount(name,count) values('"+name+"',"+count+")" 
        //实例化statement
        statement = conn.createStatement()
        
        var resultSet = statement.executeQuery(sql)
        
        if(resultSet.next()){
          statement.executeUpdate(updateSql)
        }else{
          statement.execute(insertSql)
        }
        
      })
    }catch{
      
      case e:Exception =>e.printStackTrace()
    }finally{
      if(statement !=null){
        statement.close()
      }
      if(conn !=null){
        conn.close()
      }
    }
  }
  
  /**
   * 时段浏览量输入MySQL
   */
  def myFun2(records:Iterator[Row]): Unit ={
    var conn:Connection = null
    var statement:Statement = null
    
    try{
      val url = Constants.url
      val userName:String = Constants.userName
      val passWord:String = Constants.passWord
      
      //connection连接
      conn = DriverManager.getConnection(url,userName,passWord)
      
      records.foreach(t => {
        val logtime = t.getAs[String]("logtime")
        val count = t.getAs[Long]("count").asInstanceOf[Int]
        
        val sql = "select 1 from periodcount "+" where logtime = '"+logtime+"'"
        
        val updateSql = "update periodcount set count = count+"+count+" where logtime ='"+logtime+"'"
        
        val insertSql = "insert into periodcount(logtime,count) values('"+logtime+"',"+count+")" 
        //实例化statement
        statement = conn.createStatement()
        
        var resultSet = statement.executeQuery(sql)
        
        if(resultSet.next()){
          statement.executeUpdate(updateSql)
        }else{
          statement.execute(insertSql)
        }
        
      })
    }catch{
      
      case e:Exception =>e.printStackTrace()
    }finally{
      if(statement !=null){
        statement.close()
      }
      if(conn !=null){
        conn.close()
      }
    }
  }
}