package com.aquinozz.herald.webhookdispatcher;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.security.HmacSigner;
import com.aquinozz.herald.webhookdispatcher.model.DeliveryAttempt;
import com.aquinozz.herald.webhookdispatcher.repository.DeliveryAttemptRepository;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
@Testcontainers(disabledWithoutDocker = true)
class DeliveryServiceIntegrationTests {

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
    private DeliveryAttemptRepository repository;

    @Autowired
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @AfterAll
    static void afterAll() throws IOException {
        ENDPOINT_SERVICE.shutdown();
        DESTINATION.shutdown();
    }

    @BeforeEach
    void setup() {
        repository.deleteAll();
        configurarEndpointService(DESTINATION.url("/hook").toString());
    }

    @Test
    void entregaComSucessoGravaAttemptAndAssina() throws Exception {
        DESTINATION.setDispatcher(responder(200));

        String envelope = envelope("evt-suc", 1);
        kafkaTemplate.send(KafkaTopics.EVENTS_INGRESS, "1", envelope).get();

        DeliveryAttempt attempt = aguardarAttempt();
        assertThat(attempt).isNotNull();
        assertThat(attempt.getStatus()).isEqualTo(DeliveryAttempt.Status.SUCCESS);
        assertThat(attempt.getHttpStatus()).isEqualTo(200);
        assertThat(attempt.getSignature()).isEqualTo(HmacSigner.sign(envelope, "segredo"));

        RecordedRequest recebido = DESTINATION.takeRequest();
        assertThat(recebido.getHeader("X-Webhook-Signature")).isEqualTo(attempt.getSignature());
        assertThat(recebido.getHeader("Idempotency-Key")).isEqualTo("evt-suc");
        assertThat(recebido.getBody().readUtf8()).isEqualTo(envelope);

        var contador = meterRegistry.find("herald.delivery.total")
                .tag("status", "success").counter();
        assertThat(contador).isNotNull();
        assertThat(contador.count()).isGreaterThan(0);
    }

    @Test
    void entregaComErro500GravaFailed() throws Exception {
        DESTINATION.setDispatcher(responder(500));

        kafkaTemplate.send(KafkaTopics.EVENTS_INGRESS, "1", envelope("evt-500", 1)).get();

        DeliveryAttempt attempt = aguardarAttempt();
        assertThat(attempt).isNotNull();
        assertThat(attempt.getStatus()).isEqualTo(DeliveryAttempt.Status.FAILED);
        assertThat(attempt.getHttpStatus()).isEqualTo(500);
        assertThat(attempt.getErrorMessage()).isNull();
    }

    @Test
    void entregaParaDestinoInacessivelGravaFailed() throws Exception {
        configurarEndpointService("http://localhost:1/hook");

        kafkaTemplate.send(KafkaTopics.EVENTS_INGRESS, "1", envelope("evt-conn", 1)).get();

        DeliveryAttempt attempt = aguardarAttempt();
        assertThat(attempt).isNotNull();
        assertThat(attempt.getStatus()).isEqualTo(DeliveryAttempt.Status.FAILED);
        assertThat(attempt.getErrorMessage()).isNotBlank();
    }

    private void configurarEndpointService(String destUrl) {
        ENDPOINT_SERVICE.setDispatcher(new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                if (request.getPath().endsWith("/endpoints")) {
                    String body = "[{\"id\":5,\"url\":\"" + destUrl + "\",\"ativo\":true}]";
                    return json(body);
                }
                return json("{\"id\":1,\"nome\":\"x\",\"apiKey\":\"k\",\"secretHmac\":\"segredo\",\"createdAt\":\"2026-01-01T00:00:00\"}");
            }
        });
    }

    private Dispatcher responder(int code) {
        return new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) {
                return new MockResponse().setResponseCode(code);
            }
        };
    }

    private MockResponse json(String body) {
        return new MockResponse().setHeader("Content-Type", "application/json").setBody(body);
    }

    private String envelope(String eventId, long appId) {
        return "{\"eventId\":\"" + eventId + "\",\"appId\":" + appId + ",\"type\":\"payment.confirmed\",\"data\":{\"order\":42}}";
    }

    private DeliveryAttempt aguardarAttempt() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        List<DeliveryAttempt> attempts = List.of();
        while (System.currentTimeMillis() < deadline) {
            attempts = repository.findByAppIdOrderByTimestampDesc(1L);
            if (!attempts.isEmpty()) {
                return attempts.get(0);
            }
            Thread.sleep(300);
        }
        return null;
    }
}