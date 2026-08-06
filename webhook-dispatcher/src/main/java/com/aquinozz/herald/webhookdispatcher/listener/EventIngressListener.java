package com.aquinozz.herald.webhookdispatcher.listener;

import com.aquinozz.herald.common.constants.KafkaTopics;
import com.aquinozz.herald.webhookdispatcher.service.DeliveryPlannerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Component
public class EventIngressListener {

    private static final Logger log = LoggerFactory.getLogger(EventIngressListener.class);

    private final AtomicInteger received = new AtomicInteger();
    private final DeliveryPlannerService deliveryPlannerService;

    public EventIngressListener(DeliveryPlannerService deliveryPlannerService) {
        this.deliveryPlannerService = deliveryPlannerService;
    }

    @KafkaListener(topics = KafkaTopics.EVENTS_INGRESS, groupId = "webhook-dispatcher")
    public void onEvent(String message) {
        received.incrementAndGet();
        log.info("[EventIngressListener] Evento recebido: {}", message);
        try {
            deliveryPlannerService.plan(message);
        } catch (Exception e) {
            log.error("Erro ao planejar evento: {}", e.getMessage(), e);
        }
    }

    public int countReceived() {
        return received.get();
    }
}