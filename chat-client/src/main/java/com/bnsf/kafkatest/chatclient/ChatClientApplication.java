package com.bnsf.kafkatest.chatclient;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

import com.bnsf.kafkatest.chatclient.config.KafkaConsumerProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaConsumerProperties.class)
public class ChatClientApplication {

	public static void main(String[] args) {
		SpringApplication.run(ChatClientApplication.class, args);
	}
}
