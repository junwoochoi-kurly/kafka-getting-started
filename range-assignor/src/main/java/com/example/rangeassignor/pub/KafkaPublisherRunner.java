package com.example.rangeassignor.pub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;


@Component
public class KafkaPublisherRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisherRunner.class);
    private final KafkaMessagePublisher kafkaMessagePublisher;

    public KafkaPublisherRunner(KafkaMessagePublisher kafkaMessagePublisher) {
        this.kafkaMessagePublisher = kafkaMessagePublisher;
    }

    @Override
    public void run(String... args) throws Exception {
        for (int i = 1; i <= 100; i++) {
            String jsonMessage = String.format("{\"message\":\"Hello Kafka %d\"}", i);
            kafkaMessagePublisher.sendMessage(jsonMessage);
        }

        logger.info("Published 1000 messages to Kafka");
    }

}
