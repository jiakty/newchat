package com.bnsf.kafkatest.chatclient.beans;


public class User {
	private String username;
	public User() {
		super();
		// TODO Auto-generated constructor stub
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}

	@Override
	public String toString() {
		return "User [username=" + username + "]";
	}
	
}
