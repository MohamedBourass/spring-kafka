package com.mbo.kafka;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
@DirtiesContext
@EmbeddedKafka
@TestInstance(TestInstance.Lifecycle.PER_CLASS) //This allows to use @BeforeAll annotation
@ActiveProfiles("custom")
@Slf4j
public class EmbeddedKafkaCustomIntegrationTest {

    @Autowired
    private KafkaConsumer consumer;

    @Autowired
    private KafkaProducer producer;

    @Value("${test.topic}")
    private String topic;

    @BeforeAll
    public void setup() {
        assertNotNull(this.consumer);
        assertNotNull(this.producer);
        consumer.resetLatch();
    }

    @Test
    @SneakyThrows
    public void givenEmbeddedKafka_whenSendWithProducer_thenMessageReceivedInConsumer() {
        String payload = "Send with producer";

        producer.send(topic, payload);
        log.info("Producer is sending the kafka message with payload '{}' to topic '{}'", payload,topic);

        boolean messageConsumed = consumer.getLatch()
                .await(10, TimeUnit.SECONDS);

        assertTrue(messageConsumed);

        assertThat(consumer.getPayload(), containsString(payload));
    }
}