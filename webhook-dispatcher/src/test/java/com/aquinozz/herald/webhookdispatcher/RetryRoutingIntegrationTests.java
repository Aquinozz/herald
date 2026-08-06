package com.aquinozz.herald.webhookdispatcher;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.dto.DeliveryMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "herald.retry.backoff-base-ms=10",
        "herald.retry.max-attempts=5"
})
@Testcontainers(disabledWithoutDocker = true)
class RetryRoutingIntegrationTests {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @Container
    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:7");

    static final MockWebServer ENDPOINT_SERVICE = new MockWebServer();
    static final MockWebServer DESTINATION = new MockWebServer();

    static {
        try {
            ENDPOINT_SERVICE.start();
            DESTINATION.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", () -> MONGO.getConnectionString() + "/herald");
        registry.add("herald.endpoint-service.url",
                () -> ENDPOINT_SERVICE.url("/").toString().replaceAll("/$", ""));
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @AfterAll
    static void afterAll() throws IOException {
        ENDPOINT_SERVICE.shutdown();
        DESTINATION.shutdown();
    }

    private void configurar500ComoFalha() {
        ENDPOINT_SERVICE.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                if (request.getPath().endsWith("/endpoints")) {
                    return new MockResponse().setHeader("Content-Type", "application/json")
                            .setBody("[{\"id\":1,\"url\":\"" + DESTINATION.url("/hook") + "\",\"ativo\":true}]");
                }
                return new MockResponse().setHeader("Content-Type", "application/json")
                        .setBody("{\"id\":1,\"nome\":\"x\",\"apiKey\":\"k\",\"secretHmac\":\"segredo\",\"createdAt\":\"2026-01-01T00:00:00\"}");
            }
        });
        DESTINATION.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(500);
            }
        });
    }

    @Test
    void falhaComTentativasRestantesEnviaParaRetry() throws Exception {
        configurar500ComoFalha();
        DeliveryMessage msg = new DeliveryMessage("evt-r1", 1L, 1L,
                DESTINATION.url("/hook").toString(), "payment.confirmed", "{\"x\":1}", 1, 5, 0L);

        kafkaTemplate.send(KafkaTopics.EVENTS_DELIVERY, "1",
                objectMapper.writeValueAsString(msg)).get();

        List<DeliveryMessage> retries = lerTopic(KafkaTopics.EVENTS_RETRY, 15_000);
        assertThat(retries).isNotEmpty();
        assertThat(retries.get(0).eventId()).isEqualTo("evt-r1");
        assertThat(retries.get(0).attempt()).isEqualTo(2);
    }

    @Test
    void falhaComTentativasEsgotadasEnviaParaDlq() throws Exception {
        configurar500ComoFalha();
        DeliveryMessage msg = new DeliveryMessage("evt-dlq", 1L, 1L,
                DESTINATION.url("/hook").toString(), "payment.confirmed", "{\"x\":1}", 5, 5, 0L);

        kafkaTemplate.send(KafkaTopics.EVENTS_DELIVERY, "1",
                objectMapper.writeValueAsString(msg)).get();

        List<DeliveryMessage> dlq = lerTopic(KafkaTopics.EVENTS_DLQ, 15_000);
        assertThat(dlq).isNotEmpty();
        assertThat(dlq.get(0).eventId()).isEqualTo("evt-dlq");
    }

    private List<DeliveryMessage> lerTopic(String topic, long timeoutMs) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-" + System.currentTimeMillis());
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