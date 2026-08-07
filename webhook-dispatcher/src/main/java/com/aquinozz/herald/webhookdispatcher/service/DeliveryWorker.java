package com.aquinozz.herald.webhookdispatcher.service;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.dto.DeliveryMessage;
import com.aquinozz.herald.common.security.HmacSigner;
import com.aquinozz.herald.webhookdispatcher.client.EndpointServiceClient;
import com.aquinozz.herald.webhookdispatcher.dtos.AppInfo;
import com.aquinozz.herald.webhookdispatcher.model.DeliveryAttempt;
import com.aquinozz.herald.webhookdispatcher.repository.DeliveryAttemptRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class DeliveryWorker {

    private static final Logger log = LoggerFactory.getLogger(DeliveryWorker.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final DeliveryAttemptRepository deliveryAttemptRepository;
    private final EndpointServiceClient endpointServiceClient;
    private final MeterRegistry meterRegistry;
    private final Counter retryScheduled;
    private final Counter dlqTotal;
    private final Timer deliveryTimer;

    @Value("${herald.retry.backoff-base-ms:10000}")
    private long backoffBaseMs;

    public DeliveryWorker(KafkaTemplate<String, String> kafkaTemplate,
                          WebClient webClient,
                          ObjectMapper objectMapper,
                          DeliveryAttemptRepository deliveryAttemptRepository,
                          EndpointServiceClient endpointServiceClient,
                          MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.webClient = webClient;
        this.objectMapper = objectMapper;
        this.deliveryAttemptRepository = deliveryAttemptRepository;
        this.endpointServiceClient = endpointServiceClient;
        this.meterRegistry = meterRegistry;
        this.retryScheduled = meterRegistry.counter("herald.retry.scheduled.total");
        this.dlqTotal = meterRegistry.counter("herald.dlq.total");
        this.deliveryTimer = Timer.builder("herald.delivery.duration")
                .description("Tempo de cada entrega HTTP")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    @KafkaListener(topics = KafkaTopics.EVENTS_DELIVERY, groupId = "webhook-dispatcher-delivery")
    public void onDelivery(String messageJson) {
        DeliveryMessage dm;
        try {
            dm = objectMapper.readValue(messageJson, DeliveryMessage.class);
        } catch (Exception e) {
            log.error("Mensagem de delivery invalida: {}", messageJson);
            return;
        }

        if (jaEntregue(dm.eventId(), dm.endpointId())) {
            log.info("Dedup: evento {} já entregue ao endpoint {}", dm.eventId(), dm.endpointId());
            return;
        }

        AppInfo app;
        try {
            app = endpointServiceClient.getApp(dm.appId());
        } catch (Exception e) {
            log.error("Falha ao buscar app {}: {}", dm.appId(), e.getMessage());
            retryOuDlq(dm, "falha ao resolver app");
            return;
        }
        if (app == null) {
            log.warn("App {} não encontrado - enviando para DLQ", dm.appId());
            publicarDlq(dm, "app não encontrado");
            return;
        }

        String signature = HmacSigner.sign(dm.payload(), app.secretHmac());

        DeliveryAttempt attempt = novoAttempt(dm, signature);

        long inicio = System.nanoTime();
        boolean sucesso = false;
        try {
            var response = webClient.post()
                    .uri(dm.endpointUrl())
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .header("X-Webhook-Signature", signature)
                    .header("Idempotency-Key", dm.eventId())
                    .bodyValue(dm.payload())
                    .exchangeToMono(resp -> Mono.just(resp))
                    .block(TIMEOUT);

            int statusCode = response.statusCode().value();
            attempt.setHttpStatus(statusCode);
            if (response.statusCode().is2xxSuccessful()) {
                attempt.setStatus(DeliveryAttempt.Status.SUCCESS);
                sucesso = true;
                log.info("Entrega OK (tentativa {}) para {}: {}", dm.attempt(), dm.endpointUrl(), dm.eventId());
            } else {
                attempt.setStatus(DeliveryAttempt.Status.FAILED);
                log.warn("Entrega FALHOU (http {}, tentativa {}) para {}: {}",
                        statusCode, dm.attempt(), dm.endpointUrl(), dm.eventId());
            }
        } catch (Exception e) {
            attempt.setStatus(DeliveryAttempt.Status.FAILED);
            attempt.setErrorMessage(e.getClass().getSimpleName() + ": " + e.getMessage());
            log.warn("Entrega FALHOU (tentativa {}) para {}: {}",
                    dm.attempt(), dm.endpointUrl(), e.getMessage());
        }
        attempt.setLatencyMs(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - inicio));

        deliveryTimer.record(Duration.ofMillis(attempt.getLatencyMs()));
        meterRegistry.counter("herald.delivery.total",
                        "status", attempt.getStatus() == DeliveryAttempt.Status.SUCCESS ? "success" : "failed",
                        "appId", String.valueOf(dm.appId()))
                .increment();

        try {
            deliveryAttemptRepository.save(attempt);
        } catch (Exception e) {
            log.error("Falha ao gravar DeliveryAttempt no Mongo: {}", e.getMessage());
        }

        if (!sucesso) {
            retryOuDlq(dm, attempt.getErrorMessage() == null
                    ? "http " + attempt.getHttpStatus()
                    : attempt.getErrorMessage());
        }
    }

    private void retryOuDlq(DeliveryMessage dm, String motivo) {
        if (dm.attempt() >= dm.maxAttempts()) {
            log.warn("Evento {} exauriu {} tentativas - enviando para DLQ", dm.eventId(), dm.maxAttempts());
            publicarDlq(dm, "max attempts (" + dm.maxAttempts() + ") atingido - " + motivo);
            return;
        }
        int nextAttempt = dm.attempt() + 1;
        long delayMs = backoffBaseMs * (1L << (dm.attempt() - 1));
        DeliveryMessage retry = new DeliveryMessage(
                dm.eventId(), dm.appId(), dm.endpointId(), dm.endpointUrl(),
                dm.type(), dm.payload(), nextAttempt, dm.maxAttempts(), delayMs);
        try {
            kafkaTemplate.send(KafkaTopics.EVENTS_RETRY, String.valueOf(dm.appId()),
                    objectMapper.writeValueAsString(retry));
            retryScheduled.increment();
            log.info("Agendada retentativa {} do evento {} p/ {} em {}ms",
                    nextAttempt, dm.eventId(), dm.endpointUrl(), delayMs);
        } catch (Exception e) {
            log.error("Erro ao publicar retry: {}", e.getMessage());
        }
    }

    private void publicarDlq(DeliveryMessage dm, String motivo) {
        DeliveryMessage dlq = new DeliveryMessage(
                dm.eventId(), dm.appId(), dm.endpointId(), dm.endpointUrl(),
                dm.type(), dm.payload(), dm.attempt(), dm.maxAttempts(), 0L);
        try {
            kafkaTemplate.send(KafkaTopics.EVENTS_DLQ, String.valueOf(dm.appId()),
                    objectMapper.writeValueAsString(dlq));
            dlqTotal.increment();
            log.warn("Evento {} enviado para DLQ ({})", dm.eventId(), motivo);
        } catch (Exception e) {
            log.error("Erro ao publicar DLQ: {}", e.getMessage());
        }
    }

    private boolean jaEntregue(String eventId, Long endpointId) {
        return deliveryAttemptRepository
                .findFirstByEventIdAndEndpointIdAndStatus(eventId, endpointId, DeliveryAttempt.Status.SUCCESS)
                .isPresent();
    }

    private DeliveryAttempt novoAttempt(DeliveryMessage dm, String signature) {
        DeliveryAttempt attempt = new DeliveryAttempt();
        attempt.setDeliveryId(UUID.randomUUID().toString());
        attempt.setEventId(dm.eventId());
        attempt.setAppId(dm.appId());
        attempt.setEndpointId(dm.endpointId());
        attempt.setUrl(dm.endpointUrl());
        attempt.setEventType(dm.type());
        attempt.setPayload(dm.payload());
        attempt.setSignature(signature);
        attempt.setAttempts(dm.attempt());
        attempt.setTimestamp(LocalDateTime.now());
        return attempt;
    }
}