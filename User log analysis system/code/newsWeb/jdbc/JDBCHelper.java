package com.dsj.web.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;


public class JDBCHelper {
	
	//��һ������������
	static{
		try {
			ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//�ڶ�����ʵ��JDBCHelper�ĵ�����
	private static JDBCHelper instance = null;
	
	public static JDBCHelper getInstance(){
		if(instance == null){
			synchronized (JDBCHelper.class) {
				if(instance == null){
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}
	
	//���������������ݿ����ӳ�
	private LinkedList<Connection> datasource = new LinkedList<Connection>();
	
	private JDBCHelper(){
		int size = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
		for(int i = 0; i < size; i++){
			String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
			String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
			String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			try {
				Connection conn = DriverManager.getConnection(url,user,password);
				datasource.push(conn);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	//���Ĳ����ṩ��ȡ���ݿ����ӳ�
	public synchronized Connection getConnection(){
		while(datasource.size() == 0){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}
	
	//���岽��ִ�в�ѯSQL���
	public synchronized void executeQuery(String sql,Object[] params,QueryCallback callback) throws Exception{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			pstmt =  conn.prepareStatement(sql);
			if(null != params){
				for(int i = 0 ; i < params.length ; i++){
					pstmt.setObject(i+1, params[i]);
				}
			}
			rs = pstmt.executeQuery();
			callback.process(rs);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(conn != null){
				datasource.push(conn);
			}
		}
	}
	public static interface QueryCallback{
		void process(ResultSet rs) throws Exception;
	}
}
