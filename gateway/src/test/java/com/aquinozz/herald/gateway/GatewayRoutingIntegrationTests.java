package com.aquinozz.herald.gateway;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.UUID;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
@AutoConfigureWebTestClient
class GatewayRoutingIntegrationTests {

    private static final MockWebServer BACKEND = new MockWebServer();

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @Autowired
    private WebTestClient webTestClient;

    static {
        try {
            BACKEND.setDispatcher(new Dispatcher() {
                @Override
                public MockResponse dispatch(RecordedRequest request) {
                    return new MockResponse().setResponseCode(200)
                            .setHeader("Content-Type", "application/json")
                            .setBody("{\"echo\":\"ok\"}");
                }
            });
            BACKEND.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        registry.add("HERALD_ENDPOINT_SERVICE_URI", () -> BACKEND.url("/").toString());
        registry.add("herald.rate-limit.replenish-rate", () -> 1);
        registry.add("herald.rate-limit.burst-capacity", () -> 1);
    }

    @Test
    void encaminhaParaBackendCom200() {
        webTestClient.get().uri("/api/v1/apps")
                .header("X-App-Key", chaveNova())
                .exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.echo").isEqualTo("ok");
    }

    @Test
    void excedeCotaRetorna429ComRetryAfter() {
        String chave = chaveNova();
        webTestClient.get().uri("/api/v1/apps")
                .header("X-App-Key", chave)
                .exchange()
                .expectStatus().isOk();

        webTestClient.get().uri("/api/v1/apps")
                .header("X-App-Key", chave)
                .exchange()
                .expectStatus().isEqualTo(429)
                .expectHeader().exists(HttpHeaders.RETRY_AFTER);
    }

    @Test
    void appsComKeyDiferenteNaoCompartilhamCota() {
        webTestClient.get().uri("/api/v1/apps")
                .header("X-App-Key", chaveNova())
                .exchange()
                .expectStatus().isOk();
        webTestClient.get().uri("/api/v1/apps")
                .header("X-App-Key", chaveNova())
                .exchange()
                .expectStatus().isOk();
    }

    private String chaveNova() {
        return "app-" + UUID.randomUUID();
    }
}