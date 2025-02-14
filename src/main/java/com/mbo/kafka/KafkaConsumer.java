package com.mbo.kafka;

import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Profile("custom")
@Slf4j
public class KafkaConsumer {

    private CountDownLatch latch = new CountDownLatch(1);

    private String payload;

    @KafkaListener(topics = "${test.topic}", groupId = "${test.groupid}")
    public void receive(ConsumerRecord<?, ?> consumerRecord) {
        log.info("received payload='{}'", consumerRecord.toString());

        payload = consumerRecord.toString();
        latch.countDown();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void resetLatch() {
        latch = new CountDownLatch(1);
    }

    public String getPayload() {
        return payload;
    }

}
