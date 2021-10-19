package com.emqx.util;

import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import com.emqx.dao.DBUtilsTest;

public class EmqxDatabaseUtil {

	public static DataSource getDataSource() throws IOException {
		Properties property = new Properties();// 流文件

		property.load(EmqxDatabaseUtil.class.getClassLoader().getResourceAsStream("database.properties"));

		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName(property.getProperty("jdbc.driver"));
		dataSource.setUrl(property.getProperty("jdbc.url"));
		dataSource.setUsername(property.getProperty("jdbc.username"));
		dataSource.setPassword(property.getProperty("jdbc.password"));
		
		return dataSource;
	}
}
