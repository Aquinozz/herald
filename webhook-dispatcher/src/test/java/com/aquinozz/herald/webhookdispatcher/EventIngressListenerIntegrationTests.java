package com.aquinozz.herald.webhookdispatcher;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.webhookdispatcher.listener.EventIngressListener;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class EventIngressListenerIntegrationTests {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.auto-offset-reset", () -> "earliest");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private EventIngressListener listener;

    @Test
    void consumidorRecebeEventoPublicado() throws Exception {
        kafkaTemplate.send(KafkaTopics.EVENTS_INGRESS, "1", "{\"type\":\"payment.confirmed\"}").get();

        long limite = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < limite && listener.countReceived() == 0) {
            Thread.sleep(300);
        }

        assertThat(listener.countReceived()).isGreaterThan(0);
    }
}