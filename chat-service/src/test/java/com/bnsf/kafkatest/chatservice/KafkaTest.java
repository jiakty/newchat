package com.bnsf.kafkatest.chatservice;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
//import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.KafkaTestUtils;

public class KafkaTest {
    @ClassRule
    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(2, true, 2, "all.messages");

//    @BeforeClass
//    public static void setUpBeforeClass() {
//        System.setProperty("spring.kafka.bootstrap", embeddedKafka.getBrokersAsString());
//        System.setProperty("spring.cloud.stream.kafka.binder.zkNodes", embeddedKafka.getZookeeperConnectionString());
//    }
	@Test
    public void testSpringKafka() throws Exception {
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProps = new ContainerProperties("all.messages");

        final CountDownLatch latch = new CountDownLatch(4);
        containerProps.setMessageListener((AcknowledgingMessageListener<Integer, String>) (message, ack) -> {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            latch.countDown();
        });
        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProps);
        container.setBeanName("sampleConsumer");


        container.start();

        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<Integer, String>(senderProps);
        KafkaTemplate<Integer, String> template = new KafkaTemplate<>(pf);
        template.setDefaultTopic("all.messages");
        template.sendDefault(0, 0, "message1");
        template.sendDefault(0, 1, "message2");
        template.sendDefault(1, 2, "message3");
        template.sendDefault(1, 3, "message4");
        template.flush();
        assertThat(latch.await(20, TimeUnit.SECONDS)).isTrue();
        
        container.stop();
    }
	
	@Test
    public void testEmbeddedRawKafka() throws Exception {


        Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
        producer.send(new ProducerRecord<>("all.messages", 0, 0, "message0")).get();
        producer.send(new ProducerRecord<>("all.messages", 0, 1, "message1")).get();
        producer.send(new ProducerRecord<>("all.messages", 1, 2, "message2")).get();
        producer.send(new ProducerRecord<>("all.messages", 1, 3, "message3")).get();


        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
        consumerProps.put("auto.offset.reset", "earliest");

        final CountDownLatch latch = new CountDownLatch(4);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.execute(() -> {
            KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
            kafkaConsumer.subscribe(Collections.singletonList("all.messages"));
            try {
                while (true) {
                    ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<Integer, String> record : records) {
                        
                                
                        latch.countDown();
                    }
                }
            } finally {
                kafkaConsumer.close();
            }
        });

        assertThat(latch.await(90, TimeUnit.SECONDS)).isTrue();
    }
    
	@Test
    public void testSpringKafka1() throws Exception {
		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(senderProps);
		producer.send(new ProducerRecord<>("all.messages", 0, 0, "message0")).get();
		producer.send(new ProducerRecord<>("all.messages", 0, 1, "message1")).get();
		producer.send(new ProducerRecord<>("all.messages", 1, 2, "message2")).get();
		producer.send(new ProducerRecord<>("all.messages", 1, 3, "message3")).get();     
		Map<String, Object> consumerProps =
		  KafkaTestUtils.consumerProps("sampleRawConsumer", "false", embeddedKafka);
		consumerProps.put("auto.offset.reset", "earliest");   
		final CountDownLatch latch = new CountDownLatch(4);
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		executorService.execute(() -> {    
			KafkaConsumer<Integer, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
			kafkaConsumer.subscribe(Collections.singletonList("all.messages"));    
			try{       
				while(true) {            
					ConsumerRecords<Integer, String> records = kafkaConsumer.poll(100);            
					for(ConsumerRecord<Integer, String> record : records) {                                
						latch.countDown();            
					}        
				}    
			} 
			finally{        
				kafkaConsumer.close();    
			}
		});                
		assertThat(latch.await(90, TimeUnit.SECONDS)).isTrue();
	}
	
}