package com.mbo.kafka;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka
@TestInstance(TestInstance.Lifecycle.PER_CLASS) //This allows to use @BeforeAll annotation
@ActiveProfiles("embedded")
public class EmbeddedKafkaIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    public KafkaProperties kafkaProperties;

    @Autowired
    public KafkaTemplate<String, String> template;

    private String defaultTopic;

    @Autowired
    private KafkaConsumer consumer;

    @Autowired
    private KafkaProducer producer;

    @BeforeAll
    public void setup() {
        assertNotNull(this.kafkaProperties);
        assertNotNull(this.embeddedKafkaBroker);
        assertNotNull(this.template);
        assertNotNull(this.consumer);
        assertNotNull(this.producer);
        defaultTopic =  this.template.getDefaultTopic();
        consumer.subscribe(List.of(defaultTopic));
    }

    @Test
    public void givenEmbeddedKafka_whenSendWithProducer_thenMessageReceivedInConsumer() {
        String payload = "Send with producer";

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(defaultTopic, payload);
        producer.send(producerRecord);

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(2));
        assertEquals(1, consumerRecords.count());
    }

    @Test
    public void givenEmbeddedKafkaBroker_whenSendingWithDefaultTemplate_thenMessageReceived() {
        String payload = "Sending with default template";

        template.send(defaultTopic, payload);

        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(2));
        assertEquals(1, consumerRecords.count());
    }
}