package com.aquinozz.herald.webhookdispatcher.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "delivery_attempts")
@Data
public class DeliveryAttempt {

    @Id
    private String deliveryId;

    private String eventId;

    @Indexed
    private Long appId;

    private Long endpointId;

    private String url;

    private String eventType;

    private String payload;

    private String signature;

    private Status status;

    private Integer httpStatus;

    private String errorMessage;

    private long latencyMs;

    private int attempts;

    private LocalDateTime timestamp;

    public enum Status {
        SUCCESS,
        FAILED
    }
}
