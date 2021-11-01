package com.emqx.dao;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ScalarHandler;

import com.emqx.util.EmqxDatabaseUtil;

public class AuthDAO {

	public String getUserName(String userName) throws IOException, SQLException {
		QueryRunner runner = new QueryRunner(EmqxDatabaseUtil.getDataSource());
		String sql = "select password from http_user where username='"+userName+"'";
		String password =runner.query(sql, new ScalarHandler<String>());
		return password;
	}
	
	public String getClient(String clientid) throws IOException, SQLException {
		QueryRunner runner = new QueryRunner(EmqxDatabaseUtil.getDataSource());
		String sql = "select password from http_user where clientid='"+clientid+"'";
		String password =runner.query(sql, new ScalarHandler<String>());
		return password;
	}
	
	public String getUserAccess(String userName) throws IOException, SQLException {
		QueryRunner runner = new QueryRunner(EmqxDatabaseUtil.getDataSource());
		String sql = "select access from http_acl where username='"+userName+"'";
		String access =runner.query(sql, new ScalarHandler<String>());
		return access;
	}
	
	public String getUserTopic(String userName) throws IOException, SQLException {
		QueryRunner runner = new QueryRunner(EmqxDatabaseUtil.getDataSource());
		String sql = "select topic from http_acl where username='"+userName+"'";
		String topic =runner.query(sql, new ScalarHandler<String>());
		return topic;
	}
	
	public String getClientAccess(String clientid) throws IOException, SQLException {
		QueryRunner runner = new QueryRunner(EmqxDatabaseUtil.getDataSource());
		String sql = "select access from http_acl where clientid='"+clientid+"'";
		String access =runner.query(sql, new ScalarHandler<String>());
		return access;
	}
	
	public String getClientTopic(String clientid) throws IOException, SQLException {
		QueryRunner runner = new QueryRunner(EmqxDatabaseUtil.getDataSource());
		String sql = "select topic from http_acl where clientid='"+clientid+"'";
		String topic =runner.query(sql, new ScalarHandler<String>());
		return topic;
	}
}
