package com.mbo.kafka;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test class uses Testcontainers to instantiate and manage an external Apache
 * Kafka broker hosted inside a Docker container.
 *
 * Therefore, one of the prerequisites for using Testcontainers is that Docker is installed on the host running this test
 *
 */
@Testcontainers
public class KafkaTestContainersLiveTest {

    @Container
    public KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    @Autowired
    public KafkaTemplate<String, String> template;

    private KafkaProducer<String, String> producer;

    private KafkaConsumer<String, String> consumer;

    @Test
    public void givenKafkaDockerContainer_whenSendingWithDefaultTemplate_thenMessageReceived() throws Exception {

        KAFKA_CONTAINER.addExposedPort(9092);
        KAFKA_CONTAINER.start();

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1");

        producer = new KafkaProducer<>(producerProperties);
        consumer = new KafkaConsumer<>(consumerProperties);

        assertNotNull(producer);
        assertNotNull(consumer);

        String payload = "Sending with default template";

        consumer.subscribe(List.of("test"));

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test", payload);
        producer.send(producerRecord);

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(10));
        assertEquals(1, consumerRecords.count());

        KAFKA_CONTAINER.stop();
    }
}