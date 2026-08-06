package com.aquinozz.herald.retryconsumer.listener;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.dto.DeliveryMessage;
import com.aquinozz.herald.retryconsumer.model.DeadLetter;
import com.aquinozz.herald.retryconsumer.repository.DeadLetterRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    private final DeadLetterRepository deadLetterRepository;
    private final ObjectMapper objectMapper;

    public DeadLetterListener(DeadLetterRepository deadLetterRepository, ObjectMapper objectMapper) {
        this.deadLetterRepository = deadLetterRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.EVENTS_DLQ, groupId = "retry-consumer-dlq")
    public void onDeadLetter(String messageJson) {
        DeliveryMessage dm;
        try {
            dm = objectMapper.readValue(messageJson, DeliveryMessage.class);
        } catch (Exception e) {
            log.error("Mensagem de DLQ invalida: {}", messageJson);
            return;
        }

        DeadLetter dl = new DeadLetter();
        dl.setId(UUID.randomUUID().toString());
        dl.setEventId(dm.eventId());
        dl.setAppId(dm.appId());
        dl.setEndpointId(dm.endpointId());
        dl.setEndpointUrl(dm.endpointUrl());
        dl.setType(dm.type());
        dl.setPayload(dm.payload());
        dl.setAttempts(dm.attempt());
        dl.setReason("exauriu " + dm.attempt() + "/" + dm.maxAttempts() + " tentativas");
        dl.setTimestamp(LocalDateTime.now());

        deadLetterRepository.save(dl);
        log.warn("Evento {} registrado na Dead Letter Queue", dm.eventId());
    }
}