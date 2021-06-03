package com.hadoop.java;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

/**
 * 
 * 模拟产生数据
 *
 */
public class AnalogData {
	/**
	 * 读取日志数据
	 * @param inputFile
	 * @param outputFile
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public static void readData(String inputFile,String outputFile) throws IOException, InterruptedException{
		String tmp = null;
		FileInputStream fis = null;
		InputStreamReader isr = null;
		BufferedReader br = null;
		try {
			fis = new FileInputStream(inputFile);	
			isr = new InputStreamReader(fis,"UTF-8"); 
			br = new BufferedReader(isr);
			//计数器
			int counter = 1;
			//按行读取数据
			while((tmp = br.readLine())!=null){
				System.out.println("第"+counter+"行："+tmp);
				//数据写入数据
				writeData(outputFile,tmp);
				counter++;
				//方便观察效果
				Thread.sleep(200);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(isr != null){
				isr.close();
			}
		}
	}
	/**
	 * 数据逐行写入文件
	 * @param outputFile
	 * @param line
	 * @throws FileNotFoundException 
	 */
	public static void writeData(String outputFile, String line) throws FileNotFoundException{
		BufferedWriter out = null;
		try {
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile, true)));
			out.write("\n");
			out.write(line);
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	/**
	 * 程序入口
	 * @param args
	 */
	public static void main(String[] args) {
		String inputFile = args[0];
		String outputFile = args[1];
		try {
			readData(inputFile, outputFile);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
