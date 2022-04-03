package com.haubui.kafka.stream.producer;

import com.haubui.kafka.stream.constant.KafkaConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class WordProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(WordProducer.class);

    public void sendMessage(String message) {
        kafkaTemplate.send(KafkaConstants.INPUT_TOPIC, message)
            .addCallback(
                result -> log.info("Message sent to topin : {}", message),
                ex -> log.error("Failed to send message", ex)
            );
    }
}
