package com.aquinozz.herald.endpointservice.service;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.endpointservice.dtos.EventRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class EventPublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public String publish(Long appId, EventRequest request) {
        String eventId = UUID.randomUUID().toString();

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", eventId);
        envelope.put("appId", appId);
        envelope.put("type", request.type());
        envelope.put("data", request.data());
        envelope.put("timestamp", java.time.Instant.now().toString());

        try {
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send(KafkaTopics.EVENTS_INGRESS, String.valueOf(appId), message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Erro ao serializar evento", e);
        }

        return eventId;
    }
}