package com.bnsf.kafkatest.chatservice.beans;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Message {
	private String username;
	private String message;
	private long time;
	
	@JsonCreator
    public Message(@JsonProperty("user") String username,
                    @JsonProperty("message") String message) {
        this.username = username;
        this.message = message;
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
