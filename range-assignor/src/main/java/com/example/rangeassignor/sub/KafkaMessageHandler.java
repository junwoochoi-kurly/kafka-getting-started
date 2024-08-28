package com.example.rangeassignor.sub;

import com.example.rangeassignor.domain.EventDomain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component("simpleKafkaMessageHandler")
public class KafkaMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);

    @ServiceActivator(inputChannel = "testQueueChannel", poller = @Poller(fixedDelay = "1000", taskExecutor = "taskExecutorCustom"), outputChannel = "testOutBoundChannel")
    public EventDomain handleKafkaMessage1(Message<EventDomain> message) throws InterruptedException {
        EventDomain payload = message.getPayload();
        logger.info("Received message (QueueChannel): {}", payload.getMessage());

        Thread.sleep(2000L);

        logger.info("finished (QueueChannel): {}", payload.getMessage());

        return payload;
    }

    
}
