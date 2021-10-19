package com.emqx.dao;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.commons.dbutils.handlers.columns.StringColumnHandler;


public class DBUtilsTest {

	public static void main(String args[]) throws FileNotFoundException, IOException, SQLException {
		Properties property = new Properties();//流文件  
		
		property.load(DBUtilsTest.class.getClassLoader().getResourceAsStream("database.properties"));
		
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(property.getProperty("jdbc.driver"));
		dataSource.setUrl(property.getProperty("jdbc.url"));
		dataSource.setUsername(property.getProperty("jdbc.username"));
		dataSource.setPassword(property.getProperty("jdbc.password"));

		// 初始化连接数 if(initialSize!=null)
		//dataSource.setInitialSize(Integer.parseInt(initialSize));

		// 最小空闲连接 if(minIdle!=null)
		//dataSource.setMinIdle(Integer.parseInt(minIdle));

		// 最大空闲连接 if(maxIdle!=null)
		//dataSource.setMaxIdle(Integer.parseInt(maxIdle));

		QueryRunner runner = new QueryRunner(dataSource);
		String sql="select username from mqtt_user where id=1";
		String result = runner.query(sql, new ScalarHandler<String>());
		
		System.out.println(result);
				
	}
}
