package com.aquinozz.herald.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.http.HttpHeaders;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;

public class HeraldRateLimiter extends RedisRateLimiter {

    private final long retryAfterSeconds;

    public HeraldRateLimiter(int replenishRate, int burstCapacity, long retryAfterSeconds) {
        super(replenishRate, burstCapacity);
        this.retryAfterSeconds = retryAfterSeconds;
    }

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        return super.isAllowed(routeId, id)
                .map(response -> {
                    if (!response.isAllowed()) {
                        Map<String, String> headers = new HashMap<>(response.getHeaders());
                        headers.put(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfterSeconds));
                        return new Response(false, headers);
                    }
                    return response;
                });
    }
}