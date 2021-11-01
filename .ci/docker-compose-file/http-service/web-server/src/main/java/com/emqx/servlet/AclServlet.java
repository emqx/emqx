package com.emqx.servlet;

import java.io.IOException;
import java.sql.SQLException;

import com.emqx.dao.AuthDAO;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class AclServlet extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(req, resp);
	}
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String clientid = req.getParameter("clientid");
		String username = req.getParameter("username");
		String access = req.getParameter("access");
		String topic = req.getParameter("topic");
		//String password = req.getParameter("password");
		
		//step0: password is not null, or not pass.
		
		AuthDAO dao = new AuthDAO();
		try {
			//step1: check username access&topic
			if(username != null) {
				String access_1 = dao.getUserAccess(username);
				String topic_1 = dao.getUserTopic(username);
				
				if(access.equals(access_1)) {
					if(topic.equals(topic_1)) {
						resp.setStatus(200);
					}
					else {
						if(clientid != null){
							String access_2 = dao.getClientAccess(clientid);
							String topic_2 = dao.getClientTopic(clientid);
								if(access.equals(access_2)) {
									if(topic.equals(topic_2)) {
										resp.setStatus(200);
									}
									else {
										resp.setStatus(400);
									}
								}else {
									resp.setStatus(400);
								}
						}else {
							resp.setStatus(400);
						}
					}
				}else {//step2.1: username password is not match, then check clientid password
					if(clientid != null){
						String access_3 = dao.getClientAccess(clientid);
						String topic_3 = dao.getClientTopic(clientid);
							if(access.equals(access_3)) {
								if(topic.equals(topic_3)) {
									resp.setStatus(200);
								}
								else {
									resp.setStatus(400);
								}
							}else {
								resp.setStatus(400);
							}
					}else {
						resp.setStatus(400);
					}
				}
			}else {//step2.2: username is null, then check clientid password
				if(clientid != null){
					String access_4 = dao.getClientAccess(clientid);
					String topic_4 = dao.getClientTopic(clientid);
						if(access.equals(access_4)) {
							if(topic.equals(topic_4)) {
								resp.setStatus(200);
							}
							else {
								resp.setStatus(400);
							}
						}else {
							resp.setStatus(400);
						}
				}else {
					resp.setStatus(400);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
