package com.mbo.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test class uses Testcontainers to instantiate and manage an external Apache
 * Kafka broker hosted inside a Docker container.
 *
 * Beware! The prerequisite for using Testcontainers is that Docker is installed on the host running this test
 *
 */
//@SpringBootTest
@Testcontainers
@Slf4j
public class KafkaTestContainersLiveTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    private static Admin admin;

    private static KafkaProducer<Integer, String> producer;

    private static  KafkaConsumer<Integer, String> consumer;

    private static final String PARTITION_TOPIC = "test";

    private static final int MULTIPLE_PARTITIONS = 5;

    private static final short REPLICATION_FACTOR = 1;

    private static final Duration TIMEOUT_WAIT_FOR_MESSAGES = Duration.ofSeconds(20);

    @BeforeAll
    @SneakyThrows
    public static void setUp() {
        log.info("Start Kafka container...");
        KAFKA_CONTAINER.addExposedPort(9092);


        Properties adminProperties = new Properties();
        adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        admin = Admin.create(adminProperties);
        admin.createTopics(
                ImmutableList.of(new NewTopic(PARTITION_TOPIC, MULTIPLE_PARTITIONS, REPLICATION_FACTOR)))
                .all()
                .get();

        Properties producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProperties);

        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "container-group");
        consumer = new KafkaConsumer<>(consumerProperties);

        assertNotNull(admin);
        assertNotNull(producer);
        assertNotNull(consumer);
    }

    @AfterAll
    public static void destroy() {
        log.info("Stop Kafka container...");
        KAFKA_CONTAINER.stop();
    }

    @Test
    @SneakyThrows
    public void givenKafkaContainer_whenSendWithProducer_thenMessageReceivedInConsumer() {

        List<String> sentMsgList = new ArrayList<>();
        List<String> receivedMsgList = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            String sentMsg = "ID-" + UUID.randomUUID();
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(PARTITION_TOPIC, i, sentMsg);
            Future<RecordMetadata> future = producer.send(producerRecord);
            sentMsgList.add(sentMsg);
            RecordMetadata metadata = future.get();
            log.info("Msg ID : {}, Partition : {}", sentMsg, metadata.partition());
        }

        consumer.subscribe(List.of(PARTITION_TOPIC));
        ConsumerRecords<Integer, String> consumerRecords = consumer.poll(TIMEOUT_WAIT_FOR_MESSAGES);
        consumerRecords.forEach(record -> {
            String receivedMsg = record.value();
            receivedMsgList.add(receivedMsg);
            log.info("Msg ID: " + receivedMsg);
        });

        assertThat(receivedMsgList).containsExactlyInAnyOrderElementsOf(sentMsgList);
        //assertEquals(1, consumerRecords.count());


    }
}