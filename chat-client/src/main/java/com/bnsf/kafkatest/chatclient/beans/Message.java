package com.bnsf.kafkatest.chatclient.beans;

public class Message {
	private String username;
	private String message;
	private long time;
	public Message() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public String getUser() {
		return username;
	}
	public void setUser(String user) {
		this.username = user;
	}
	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	@Override
	public String toString() {
		return "Message [username=" + username + ", message=" + message + ", time=" + time + "]";
	}
	
}
