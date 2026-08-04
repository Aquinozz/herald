package com.aquinozz.herald.webhookdispatcher.dtos;

import java.time.LocalDateTime;

public record AppInfo(
        Long id,
        String nome,
        String apiKey,
        String secretHmac,
        LocalDateTime createdAt) {
}
