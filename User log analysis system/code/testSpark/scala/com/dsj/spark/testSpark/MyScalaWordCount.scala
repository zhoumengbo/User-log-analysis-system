package com.dsj.spark.testSpark

import org.apache.spark.SparkContext
import org.apache.spark.deploy.SparkCuratorUtil
import org.apache.spark.SparkConf

object MyScalaWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length<2){
      System.err.println("Usage:MyScalaWordCount<input> <output>")
      System.exit(1)
    }
    
    //输入路径
    val input = args(0)
    //输出路径
    val output = args(1)
    
    val conf = new SparkConf().setAppName("MyScalaWordCount")
   
    val sc = new SparkContext(conf)
    
    //读取数据
    val line = sc.textFile(input)
    
    val resultRDD = line.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_)
    
    //保存结果
    resultRDD.saveAsTextFile(output)
    
    sc.stop()
  }
}