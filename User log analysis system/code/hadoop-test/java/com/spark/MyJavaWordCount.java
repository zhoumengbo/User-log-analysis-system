package com.spark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 
 * java版本的Wordcount
 *
 */
public class MyJavaWordCount {

	public static void main(String[] args) {
		//参数检查
		if(args.length<2){
			System.err.println("Usage:MyJavaWordCount input ouput");
			System.exit(1);
		}
		
		//输入路径
		String inputPath = args[0];
		//输出路径
		String outputPath = args[1];
		
		//创建SparkContext
		SparkConf conf = new SparkConf().setAppName("MyJavaWordCount");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		//读取数据
		JavaRDD<String> inputRDD = jsc.textFile(inputPath);
		
		//flatmap扁平化操作
		JavaRDD<String> words = inputRDD.flatMap(new FlatMapFunction<String, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Iterator<String> call(String line) throws Exception {
				
				return Arrays.asList(line.split("\\s+")).iterator();
			}
		});
		
		//map 操作
		JavaPairRDD<String, Integer> pairPDD = words.mapToPair(new PairFunction<String, String, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				
				return new Tuple2<String, Integer>(word,1);
			}
		});
		JavaPairRDD<String, Integer> result = pairPDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public Integer call(Integer x, Integer y) throws Exception {
				
				return x+y;
			}
		});
		
		//保存结果
		result.saveAsTextFile(outputPath);
		
		//关闭SparkContext
		jsc.close();
	}
}
