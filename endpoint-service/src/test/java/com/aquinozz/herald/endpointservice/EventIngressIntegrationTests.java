package com.aquinozz.herald.endpointservice;

import com.aquinozz.herald.common.constants.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class EventIngressIntegrationTests {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void publicarEventoRetorna202EEntraNoTopico() {
        Long appId = criarApp();
        String eventType = "payment.confirmed";

        ResponseEntity<Map> response = post("/api/v1/apps/" + appId + "/events",
                "{\"type\":\"" + eventType + "\",\"data\":{\"order\":42}}");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(response.getBody().get("status")).isEqualTo("RECEIVED");
        assertThat((String) response.getBody().get("eventId")).isNotBlank();

        boolean encontrou = KafkaHelper.aguardarEvento(KAFKA, appId, eventType);
        assertThat(encontrou).isTrue();
    }
    @Test
    void publicarEventoDeAppInexistenteRetorna404() {
        ResponseEntity<Map> response = post("/api/v1/apps/9999/events",
                "{\"type\":\"payment.confirmed\"}");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void publicarEventoComTypeVazioRetorna400() {
        Long appId = criarApp();
        ResponseEntity<Map> response = post("/api/v1/apps/" + appId + "/events", "{\"type\":\"\"}");
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    private Long criarApp() {
        ResponseEntity<Map> response = post("/api/v1/apps", "{\"nome\":\"Loja Kafka\"}");
        return ((Number) response.getBody().get("id")).longValue();
    }

    private ResponseEntity<Map> post(String url, String json) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return restTemplate.postForEntity(url, new HttpEntity<>(json, headers), Map.class);
    }
}

final class KafkaHelper {

    static boolean aguardarEvento(KafkaContainer kafka, Long appId, String type) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + System.currentTimeMillis());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(KafkaTopics.EVENTS_INGRESS));
            long limite = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < limite) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (String.valueOf(appId).equals(record.key())
                            && record.value() != null
                            && record.value().contains(type)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}