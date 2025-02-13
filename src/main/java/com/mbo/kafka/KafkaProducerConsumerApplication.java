package com.mbo.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaProducerConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaProducerConsumerApplication.class, args);
    }

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public KafkaConsumer<String, String> consumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties.buildConsumerProperties(null));
        return consumer;
    }

    @Bean
    public KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(kafkaProperties.buildProducerProperties(null));
    }
}
