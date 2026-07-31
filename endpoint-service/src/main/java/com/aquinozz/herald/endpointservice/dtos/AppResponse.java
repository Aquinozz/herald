package com.aquinozz.herald.endpointservice.dtos;

import com.aquinozz.herald.endpointservice.model.App;

import java.time.LocalDateTime;

public record AppResponse(Long id, String nome, String apiKey, String secretHmac, LocalDateTime createdAt) {

    public static AppResponse from(App app) {
        return new AppResponse(
                app.getId(),
                app.getNome(),
                app.getApiKey(),
                app.getSecretHmac(),
                app.getCreatedAt());
    }
}
