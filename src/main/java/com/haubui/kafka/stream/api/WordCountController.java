package com.haubui.kafka.stream.api;

import com.haubui.kafka.stream.producer.WordProducer;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

@RestController
public class WordCountController {

    @Autowired
    private StreamsBuilderFactoryBean factoryBean;

    @Autowired
    private WordProducer wordProducer;

    @GetMapping("/count/{word}")
    public Long getWordCount(@PathVariable String word) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
            .store(StoreQueryParameters.fromNameAndType("counts", QueryableStoreTypes.keyValueStore()));
        return counts.get(word) != null ? counts.get(word) : 0;
    }

    @PostMapping("/message")
    public void addMessage(@RequestBody String message) {
        wordProducer.sendMessage(message);
    }
}
