package com.emqx.servlet;

import java.io.IOException;
import java.sql.SQLException;

import com.emqx.dao.AuthDAO;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class AuthServlet extends HttpServlet {

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		// TODO Auto-generated method stub
		doPost(req, resp);
	}
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String clientid = req.getParameter("clientid");
		String username =req.getParameter("username");
		String password = req.getParameter("password");
		
		//step0: password is not null, or not pass.
		if(password == null) {
			resp.setStatus(400);
			return;
		}
		AuthDAO dao = new AuthDAO();
		try {
			//step1: check username password
			if(username != null) {
				String password_d = dao.getUserName(username);
				
				if(password.equals(password_d)) {
					resp.setStatus(200);
					//200
				}else {//step2.1: username password is not match, then check clientid password
					if(clientid != null){
						String password_c = dao.getClient(clientid);
							if(password.equals(password_c)) {
								resp.setStatus(200);
							}else {
								resp.setStatus(400);
							}
					}else {
						resp.setStatus(400);
					}
				}
			}else {//step2.2: username is null, then check clientid password
				if(clientid != null){
					String password_c = dao.getClient(clientid);
						if(password.equals(password_c)) {
							resp.setStatus(200);
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
