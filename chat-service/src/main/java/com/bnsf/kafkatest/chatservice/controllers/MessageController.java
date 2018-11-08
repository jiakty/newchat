package com.bnsf.kafkatest.chatservice.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.bnsf.kafkatest.chatservice.Response;
import com.bnsf.kafkatest.chatservice.beans.Message;
import com.bnsf.kafkatest.chatservice.beans.User;
import com.bnsf.kafkatest.chatservice.service.MessageService;
@RestController
@RequestMapping("/MC")
public class MessageController {
	@Autowired
	private MessageService ms;
	
	@PostMapping("/send")
	public Response sendToAll(Message message) {
		System.out.println("test" + message);
		return this.ms.sendMessage(message);
	}
}
