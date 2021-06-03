package com.dsj.spark.structedStreaming

import java.sql.{Connection, DriverManager}
import java.util

class MySqlPool(url:String, user:String, pwd:String) extends Serializable{
  private val max = 5               //连接池连接总数
  private var conNum = 0            //当前连接池已产生的连接数
  private val pool = new util.LinkedList[Connection]()    //连接池

  //获取连接
  def getJdbcConn() : Connection = {
    //同步代码块
    AnyRef.synchronized({
      //线程池如果为空
      if(pool.isEmpty){
        //加载驱动
        preGetConn()
        //获取conn连接
        val conn = DriverManager.getConnection(url,user,pwd)
        //conn连接加入线程池
        pool.push(conn)
        conNum +=  1
      }
      pool.poll()
    })
  }

  //释放连接
  def releaseConn(conn:Connection): Unit ={
    pool.push(conn)
  }
  
  //加载驱动
  private def preGetConn() : Unit = {
    //控制加载
    if(conNum < max && !pool.isEmpty){
      Thread.sleep(2000)
      preGetConn()
    }else{
      Class.forName("com.mysql.jdbc.Driver");
    }
  }

}
