package com.haubui.kafka.stream.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import static com.haubui.kafka.stream.constant.KafkaConstants.OUTPUT_TOPIC;

@Component
public class WordConsumer {

    private static final Logger log = LoggerFactory.getLogger(WordConsumer.class);

    @KafkaListener(topics = {OUTPUT_TOPIC}, groupId = "kafka-stream")
    public void consume(ConsumerRecord<String, Long> record) {
        log.info("received = {} with key {}", record.value(), record.key());
    }
}
