package com.aquinozz.herald.webhookdispatcher.service;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.dto.DeliveryMessage;
import com.aquinozz.herald.webhookdispatcher.client.EndpointServiceClient;
import com.aquinozz.herald.webhookdispatcher.dtos.EndpointInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DeliveryPlannerService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryPlannerService.class);

    private final EndpointServiceClient endpointServiceClient;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${herald.retry.max-attempts:5}")
    private int maxAttempts;

    public DeliveryPlannerService(EndpointServiceClient endpointServiceClient,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.endpointServiceClient = endpointServiceClient;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void plan(String rawEvent) {
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

        List<EndpointInfo> endpoints;
        try {
            endpoints = endpointServiceClient.getActiveEndpoints(appId);
        } catch (Exception e) {
            log.error("Falha ao buscar endpoints do app {}: {}", appId, e.getMessage());
            return;
        }

        if (endpoints.isEmpty()) {
            log.info("App {} sem endpoints ativos - evento {} descartado", appId, eventId);
            return;
        }

        for (EndpointInfo endpoint : endpoints) {
            DeliveryMessage msg = new DeliveryMessage(
                    eventId,
                    appId,
                    endpoint.id(),
                    endpoint.url(),
                    type,
                    rawEvent,
                    1,
                    maxAttempts,
                    0L);
            try {
                String json = objectMapper.writeValueAsString(msg);
                kafkaTemplate.send(KafkaTopics.EVENTS_DELIVERY, String.valueOf(appId), json);
                log.info("Planejada entrega p/ endpoint {} ({}): {}", endpoint.id(), endpoint.url(), eventId);
            } catch (Exception e) {
                log.error("Erro ao publicar delivery p/ endpoint {}: {}", endpoint.id(), e.getMessage());
            }
        }
    }
}