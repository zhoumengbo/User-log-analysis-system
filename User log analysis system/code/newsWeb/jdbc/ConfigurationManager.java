package com.dsj.web.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {
	
	private static Properties prop = new Properties();
	static{
		InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
		try {
			prop.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static String getProperty(String key){
		
		return prop.getProperty(key);
	}
	public static int getInteger(String key){
		
		return Integer.parseInt(prop.getProperty(key));
	}
	public static Long getLong(String key){
		String value = getProperty(key);
		return Long.parseLong(value);
	}
}
