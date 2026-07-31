package com.aquinozz.herald.common.constants;

public final class KafkaTopics {

    public static final String EVENTS_INGRESS = "webhook.events.ingress";
    public static final String EVENTS_DELIVERY = "webhook.events.delivery";
    public static final String EVENTS_RETRY = "webhook.events.retry";
    public static final String EVENTS_DLQ = "webhook.events.dlq";

    private KafkaTopics() {
    }
}
