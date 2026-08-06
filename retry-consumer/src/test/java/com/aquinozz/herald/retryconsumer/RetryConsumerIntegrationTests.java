package com.aquinozz.herald.retryconsumer;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.dto.DeliveryMessage;
import com.aquinozz.herald.retryconsumer.model.DeadLetter;
import com.aquinozz.herald.retryconsumer.repository.DeadLetterRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@Testcontainers(disabledWithoutDocker = true)
class RetryConsumerIntegrationTests {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7");

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", () -> MONGO.getConnectionString() + "/herald");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private DeadLetterRepository deadLetterRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void retryRepassaAoDeliveryAposDelay() throws Exception {
        DeliveryMessage msg = new DeliveryMessage("evt-retry", 1L, 1L,
                "http://localhost:1/hook", "payment.confirmed", "{\"x\":1}", 2, 5, 300L);

        kafkaTemplate.send(KafkaTopics.EVENTS_RETRY, "1",
                objectMapper.writeValueAsString(msg)).get();

        List<DeliveryMessage> delivery = lerTopic(KafkaTopics.EVENTS_DELIVERY, 15_000);
        assertThat(delivery).isNotEmpty();
        assertThat(delivery.get(0).eventId()).isEqualTo("evt-retry");
        assertThat(delivery.get(0).attempt()).isEqualTo(2);
    }

    @Test
    void dlqGeraDeadLetterNoMongo() throws Exception {
        DeliveryMessage msg = new DeliveryMessage("evt-dlq", 1L, 1L,
                "http://localhost:1/hook", "payment.confirmed", "{\"x\":1}", 5, 5, 0L);

        kafkaTemplate.send(KafkaTopics.EVENTS_DLQ, "1",
                objectMapper.writeValueAsString(msg)).get();

        DeadLetter saved = null;
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline && saved == null) {
            saved = deadLetterRepository.findAll().stream()
                    .filter(d -> "evt-dlq".equals(d.getEventId()))
                    .findFirst().orElse(null);
            if (saved == null) {
                Thread.sleep(300);
            }
        }

        assertThat(saved).isNotNull();
        assertThat(saved.getAttempts()).isEqualTo(5);
        assertThat(saved.getReason()).contains("5/5");
    }

    private List<DeliveryMessage> lerTopic(String topic, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<DeliveryMessage> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(300));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        result.add(objectMapper.readValue(record.value(), DeliveryMessage.class));
                    } catch (Exception ignored) {
                    }
                }
                if (!result.isEmpty()) {
                    break;
                }
            }
        }
        return result;
    }
}