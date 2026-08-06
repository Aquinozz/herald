package com.aquinozz.herald.retryconsumer.listener;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.common.dto.DeliveryMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class RetryListener {

    private static final Logger log = LoggerFactory.getLogger(RetryListener.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public RetryListener(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopics.EVENTS_RETRY, groupId = "retry-consumer")
    public void onRetry(String messageJson) throws InterruptedException {
        DeliveryMessage dm;
        try {
            dm = objectMapper.readValue(messageJson, DeliveryMessage.class);
        } catch (Exception e) {
            log.error("Mensagem de retry invalida: {}", messageJson);
            return;
        }

        if (dm.nextDelayMs() > 0) {
            log.info("Retentativa {} do evento {} aguardando {}ms antes de reenviar",
                    dm.attempt(), dm.eventId(), dm.nextDelayMs());
            Thread.sleep(dm.nextDelayMs());
        }

        kafkaTemplate.send(KafkaTopics.EVENTS_DELIVERY, String.valueOf(dm.appId()), messageJson);
        log.info("Reenviado evento {} ao delivery (tentativa {})", dm.eventId(), dm.attempt());
    }
}