package com.aquinozz.herald.webhookdispatcher.service;

import com.aquinozz.herald.common.security.HmacSigner;
import com.aquinozz.herald.webhookdispatcher.client.EndpointServiceClient;
import com.aquinozz.herald.webhookdispatcher.dtos.AppInfo;
import com.aquinozz.herald.webhookdispatcher.dtos.EndpointInfo;
import com.aquinozz.herald.webhookdispatcher.model.DeliveryAttempt;
import com.aquinozz.herald.webhookdispatcher.repository.DeliveryAttemptRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final EndpointServiceClient endpointServiceClient;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public DeliveryService(EndpointServiceClient endpointServiceClient,
                           DeliveryAttemptRepository deliveryAttemptRepository,
                           WebClient webClient,
                           ObjectMapper objectMapper) {
        this.endpointServiceClient = endpointServiceClient;
        this.deliveryAttemptRepository = deliveryAttemptRepository;
        this.webClient = webClient;
        this.objectMapper = objectMapper;
    }

    public void deliver(String rawEvent) {
        JsonNode event;
        try {
            event = objectMapper.readTree(rawEvent);
        } catch (Exception e) {
            log.error("Evento invalido: {}", rawEvent);
            return;
        }

        Long appId = event.path("appId").asLong();
        String eventId = event.path("eventId").asText();
        String type = event.path("type").asText();

        AppInfo app;
        try {
            app = endpointServiceClient.getApp(appId);
        } catch (Exception e) {
            log.error("Falha ao buscar app {}: {}", appId, e.getMessage());
            return;
        }
        if (app == null) {
            log.warn("App {} não encontrado", appId);
            return;
        }

        List<EndpointInfo> endpoints;
        try {
            endpoints = endpointServiceClient.getActiveEndpoints(appId);
        } catch (Exception e) {
            log.error("Falha ao buscar endpoints do app {}: {}", appId, e.getMessage());
            return;
        }

        if (endpoints.isEmpty()) {
            log.info("App {} sem endpoints ativos", appId);
            return;
        }

        for (EndpointInfo endpoint : endpoints) {
            entregar(endpoint, app, eventId, type, rawEvent);
        }
    }

    private void entregar(EndpointInfo endpoint, AppInfo app,
                          String eventId, String type, String body) {
        String signature = HmacSigner.sign(body, app.secretHmac());

        DeliveryAttempt attempt = new DeliveryAttempt();
        attempt.setDeliveryId(UUID.randomUUID().toString());
        attempt.setEventId(eventId);
        attempt.setAppId(app.id());
        attempt.setEndpointId(endpoint.id());
        attempt.setUrl(endpoint.url());
        attempt.setEventType(type);
        attempt.setPayload(body);
        attempt.setSignature(signature);
        attempt.setAttempts(1);
        attempt.setTimestamp(LocalDateTime.now());

        long inicio = System.nanoTime();
        try {
            var response = webClient.post()
                    .uri(endpoint.url())
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .header("X-Webhook-Signature", signature)
                    .header("Idempotency-Key", eventId)
                    .bodyValue(body)
                    .exchangeToMono(resp -> Mono.just(resp))
                    .block(TIMEOUT);

            int statusCode = response.statusCode().value();
            attempt.setHttpStatus(statusCode);
            if (response.statusCode().is2xxSuccessful()) {
                attempt.setStatus(DeliveryAttempt.Status.SUCCESS);
                log.info("Entrega OK para {} (http {}): {}", endpoint.url(), statusCode, eventId);
            } else {
                attempt.setStatus(DeliveryAttempt.Status.FAILED);
                log.warn("Entrega FALHOU para {} (http {}): {}", endpoint.url(), statusCode, eventId);
            }
        } catch (Exception e) {
            attempt.setStatus(DeliveryAttempt.Status.FAILED);
            attempt.setErrorMessage(e.getClass().getSimpleName() + ": " + e.getMessage());
            log.warn("Entrega FALHOU para {}: {}", endpoint.url(), e.getMessage());
        }
        attempt.setLatencyMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - inicio));

        try {
            deliveryAttemptRepository.save(attempt);
        } catch (Exception e) {
            log.error("Falha ao gravar DeliveryAttempt no Mongo: {}", e.getMessage());
        }
    }
}