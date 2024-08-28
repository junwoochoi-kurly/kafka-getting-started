package com.example.rangeassignor.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;

import java.util.Map;

public class SimpleKafkaMessageConverter extends StringJsonMessageConverter {

    private final Map<String, Class<?>> topicToClassMap;
    private final ObjectMapper objectMapper;

    public SimpleKafkaMessageConverter(ObjectMapper objectMapper, Map<String, Class<?>> topicToClassMap) {
        this.objectMapper = objectMapper;
        this.topicToClassMap = topicToClassMap;
    }

    @Override
    protected Object extractAndConvertValue(ConsumerRecord<?, ?> consumerRecord, java.lang.reflect.Type type) {
        String topic = consumerRecord.topic();
        Class<?> targetClass = topicToClassMap.get(topic);

        if (targetClass == null) {
            throw new IllegalArgumentException("No target class found for topic: " + topic);
        }

        try {
            String message = (String) consumerRecord.value();
            return objectMapper.readValue(message, targetClass);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert message", e);
        }
    }

}
