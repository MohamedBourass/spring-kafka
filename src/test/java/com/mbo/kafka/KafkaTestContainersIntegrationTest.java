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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This test class uses Testcontainers to instantiate and manage an external Apache
 * Kafka broker hosted inside a Docker container.
 *
 * Beware! The prerequisite for using Testcontainers is that Docker is installed on the host running this test
 *
 */
@Testcontainers
@Slf4j
public class KafkaTestContainersIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));

    private static Admin admin;

    private static KafkaProducer<Integer, String> producer;

    private static  KafkaConsumer<Integer, String> consumer;

    private static final String PARTITION_TOPIC = "test";

    private static final int MULTIPLE_PARTITIONS = 5;

    private static final short REPLICATION_FACTOR = 1;

    private static final Duration TIMEOUT_WAIT_FOR_MESSAGES = Duration.ofSeconds(5);

    private static String HEADER_KEY = "website";
    private static String HEADER_VALUE = "mywebsite.com";

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

        //Check the creation of the topic
        String topicCommand = "/usr/bin/kafka-topics --bootstrap-server=localhost:9092 --list";
        String stdout = KAFKA_CONTAINER.execInContainer("/bin/sh", "-c", topicCommand)
                .getStdout();

        assertThat(stdout).contains(PARTITION_TOPIC);

        log.info("Kafka container is loaded");
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

        List<Header> headers = new ArrayList<>();
        headers.add(new RecordHeader(HEADER_KEY, HEADER_VALUE.getBytes()));

        for (int msgKey = 1; msgKey <= 10; msgKey++) {
            String msgValue = "ID-" + UUID.randomUUID();
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(PARTITION_TOPIC, null, msgKey, msgValue, headers);
            Future<RecordMetadata> future = producer.send(producerRecord);
            sentMsgList.add(msgValue);
            RecordMetadata metadata = future.get();
            log.info("Message sent => Key : {}, Value : {}, Partition : {}", msgKey, msgValue, metadata.partition());
        }

        consumer.subscribe(List.of(PARTITION_TOPIC));
        ConsumerRecords<Integer, String> consumerRecords = consumer.poll(TIMEOUT_WAIT_FOR_MESSAGES);
        consumerRecords.forEach(record -> {
            Integer msgKey = record.key();
            String msgValue = record.value();
            receivedMsgList.add(msgValue);

            Headers consumedHeaders = record.headers();
            assertNotNull(consumedHeaders);
            for (Header header : consumedHeaders) {
                assertEquals(HEADER_KEY, header.key());
                assertEquals(HEADER_VALUE, new String(header.value()));
            }
            log.info("Message received => Key : {}, Value : {}, Partition : {}", msgKey, msgValue, record.partition());
        });

        assertThat(receivedMsgList).containsExactlyInAnyOrderElementsOf(sentMsgList);
    }
}