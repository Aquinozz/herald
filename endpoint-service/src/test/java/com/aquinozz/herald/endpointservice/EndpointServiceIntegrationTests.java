package com.aquinozz.herald.endpointservice;

import com.aquinozz.herald.endpointservice.dtos.AppResponse;
import com.aquinozz.herald.endpointservice.dtos.EndpointResponse;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class EndpointServiceIntegrationTests {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "create-drop");
    }

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void fluxoCompletoCriarAppEEndpoint() {
        AppResponse app = criarApp("Minha Loja");
        assertThat(app.id()).isNotNull();
        assertThat(app.apiKey()).isNotBlank();
        assertThat(app.secretHmac()).isNotBlank();

        EndpointResponse endpoint = criarEndpoint(app.id(), "https://macaco.com/webhook");
        assertThat(endpoint.id()).isNotNull();
        assertThat(endpoint.url()).isEqualTo("https://macaco.com/webhook");
        assertThat(endpoint.ativo()).isTrue();

        List<?> endpoints = listarEndpoints(app.id());
        assertThat(endpoints).hasSize(1);
    }

    @Test
    void criarAppSemNomeRetorna400() {
        ResponseEntity<Map> response = post("/api/v1/apps", "{\"nome\":\"\"}", Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void endpointDeAppInexistenteRetorna404() {
        ResponseEntity<Map> response = post("/api/v1/apps/9999/endpoints", "{\"url\":\"https://macaco.com/hook\"}", Map.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private AppResponse criarApp(String nome) {
        return post("/api/v1/apps", "{\"nome\":\"" + nome + "\"}", AppResponse.class).getBody();
    }

    private EndpointResponse criarEndpoint(Long appId, String url) {
        return post("/api/v1/apps/" + appId + "/endpoints", "{\"url\":\"" + url + "\"}", EndpointResponse.class).getBody();
    }

    private List<?> listarEndpoints(Long appId) {
        return restTemplate.getForObject("/api/v1/apps/" + appId + "/endpoints", List.class);
    }

    private <T> ResponseEntity<T> post(String url, String json, Class<T> responseType) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return restTemplate.postForEntity(url, new HttpEntity<>(json, headers), responseType);
    }
}
