package com.aquinozz.herald.gateway.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.cloud.gateway.filter.ratelimit.RedisRateLimiter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

@Configuration
public class GatewayConfig {

    @Value("${herald.rate-limit.replenish-rate:20}")
    private int replenishRate;

    @Value("${herald.rate-limit.burst-capacity:40}")
    private int burstCapacity;

    @Value("${herald.rate-limit.retry-after-seconds:5}")
    private long retryAfterSeconds;

    @Bean
    KeyResolver appKeyKeyResolver() {
        return exchange -> {
            String key = exchange.getRequest().getHeaders().getFirst("X-App-Key");
            if (key == null || key.isBlank()) {
                var remote = exchange.getRequest().getRemoteAddress();
                key = remote != null ? remote.getAddress().getHostAddress() : "anon";
            }
            return Mono.just(key);
        };
    }

    @Bean
    public RedisRateLimiter defaultRateLimiter() {
        return new HeraldRateLimiter(replenishRate, burstCapacity, retryAfterSeconds);
    }
}