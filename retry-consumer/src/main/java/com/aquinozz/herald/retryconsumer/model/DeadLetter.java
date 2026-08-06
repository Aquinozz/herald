package com.aquinozz.herald.retryconsumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

@Document(collection = "dead_letters")
@Data
public class DeadLetter {

    @Id
    private String id;

    private String eventId;

    private Long appId;

    private Long endpointId;

    private String endpointUrl;

    private String type;

    private String payload;

    private int attempts;

    private String reason;

    private LocalDateTime timestamp;
}