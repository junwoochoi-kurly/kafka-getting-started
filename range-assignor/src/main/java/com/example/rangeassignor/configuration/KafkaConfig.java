package com.example.rangeassignor.configuration;

import com.example.rangeassignor.converter.SimpleKafkaMessageConverter;
import com.example.rangeassignor.domain.EventDomain;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.messaging.PollableChannel;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;


    private static final ObjectMapper KAFKA_MAPPER;

    static {
        KAFKA_MAPPER = new ObjectMapper();
    }

    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public PollableChannel testQueueChannel() {
        return new QueueChannel();
    }

    /**
     * containerProps.setClientId("unique-client-id-" + UUID.randomUUID());
     */
    @Bean("simpleKafkaListenerContainer")
    public ConcurrentMessageListenerContainer<String, String> kafkaListenerContainer(ConsumerFactory<String, String> consumerFactory) {
        String[] topics = {"topic1"};
        ContainerProperties containerProps = new ContainerProperties(topics);
//        containerProps.setClientId("unique-client-id-" + UUID.randomUUID());
        containerProps.setGroupId("test-topic-group");

        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        container.setConcurrency(3);

        return container;
    }

    @Bean("simpleConcurrentKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerFactory(ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public SimpleKafkaMessageConverter messageConverter() {
        Map<String, Class<?>> topicMap = Map.of("topic1", EventDomain.class);
        return new SimpleKafkaMessageConverter(getKafkaMapper(), topicMap);
    }

    @Bean("simpleKafkaMessageDrivenChannelAdapter")
    public KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageAdapter(
        ConcurrentMessageListenerContainer<String, String> simpleKafkaListenerContainer,
        PollableChannel testQueueChannel,
        SimpleKafkaMessageConverter messageConverter) {

        KafkaMessageDrivenChannelAdapter<String, String> adapter = new KafkaMessageDrivenChannelAdapter<>(simpleKafkaListenerContainer);
        adapter.setOutputChannel(testQueueChannel);
        adapter.setMessageConverter(messageConverter);
        return adapter;
    }

    @Bean
    public ConsumerFactory consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Custom consumer settings
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 22 * 1024 * 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 50 * 1024 * 1024);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    public static ObjectMapper getKafkaMapper() {
        return KAFKA_MAPPER;
    }

}
