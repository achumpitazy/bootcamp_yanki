package com.bootcamp.yanki.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class KafkaStringProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStringProducer.class);

    private final KafkaProducer<String, String> kafkaProducer;
    
    public KafkaStringProducer(@Qualifier("kafkaStringTemplate") KafkaProducer<String, String> kafkaProducer) {
		this.kafkaProducer = kafkaProducer;
    }
    
    public void sendMessage(String message) {
    	ProducerRecord<String, String> record = new ProducerRecord<>("bootcamp-topic", message);
        LOGGER.info("Producing message {}", message);
        this.kafkaProducer.send(record);
    }

}