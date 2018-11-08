package com.bnsf.kafkatest.chatclient;

import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@Service
public class MessageConsumer {

    private final CountDownLatch latch;

    @Autowired
    public MessageConsumer(CountDownLatch latch) {
        this.latch = latch;
    }

    private final Map<String, String> receivedMessages = new ConcurrentHashMap<>();

    public Map<String, String> getReceivedMessages() {
        return receivedMessages;
    }

    @KafkaListener(topics = "TOPIC1", groupId = "demo-group2")
    public void onReceiving(ConsumerRecord<?, ?> consumerRecord) {
        receivedMessages.put("message", String.valueOf(consumerRecord.value()));
    	System.out.println(consumerRecord.value());
    	//System.out.println("Receiver on topic1: "+consumerRecord.toString());
    }
}

