package com.bnsf.kafkatest.chatclient;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest()
@EmbeddedKafka(topics = "TOPIC1")
public class MessageConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> template;

    @Autowired
    private MessageConsumer listener;

    @Test
    public void testReceive() throws Exception {
        ListenableFuture<SendResult<String, String>> sent = template.send("TOPIC1", "foo");
        SendResult<String, String> result = sent.get();


        // Wait until received, stored and retrieved
        await().untilAsserted(() -> assertThat(listener.getReceivedMessages().get("message")).isNotNull());

    }

}
