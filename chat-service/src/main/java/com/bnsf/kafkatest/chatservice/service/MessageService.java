package com.bnsf.kafkatest.chatservice.service;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.bnsf.kafkatest.chatservice.Response;
import com.bnsf.kafkatest.chatservice.beans.Message;
import com.bnsf.kafkatest.chatservice.config.KafkaProducerProperties;

@Service
public class MessageService {

    @Autowired
    private KafkaTemplate<String, Message> messageKafkaTemplate;

    @Autowired
    private KafkaProducerProperties kafkaProducerProperties;
    
    public boolean dispatch(Message message) {
        try {
            SendResult<String, Message> sendResult = messageKafkaTemplate.sendDefault(message.getUser(), message).get();
            RecordMetadata recordMetadata = sendResult.getRecordMetadata();
            System.out.println("offset: "+recordMetadata.offset() +", partition: "+ recordMetadata.partition()+", topic:"+recordMetadata.topic()+"    "+sendResult+"       "+recordMetadata);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Response sendMessage(Message message){

    	if(message.getUser()==null)
    		return new Response(false,"User can not be null");
    	else if(message.getMessage()==null)
    		return new Response(false, "Message can not be null");
    	else if (dispatch(message))
    		return new Response(true,"Success");
    	else
    		return new Response(false);
    }
}
