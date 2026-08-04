package com.aquinozz.herald.webhookdispatcher.client;

import com.aquinozz.herald.webhookdispatcher.dtos.AppInfo;
import com.aquinozz.herald.webhookdispatcher.dtos.EndpointInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.List;

@Component
public class EndpointServiceClient {

    private final WebClient webClient;
    private final String baseUrl;

    public EndpointServiceClient(WebClient webClient,
                                 @Value("${herald.endpoint-service.url:http://localhost:8081}") String baseUrl) {
        this.webClient = webClient;
        this.baseUrl = baseUrl;
    }

    public AppInfo getApp(Long appId) {
        return webClient.get()
                .uri(baseUrl + "/api/v1/apps/{id}", appId)
                .retrieve()
                .bodyToMono(AppInfo.class)
                .block(Duration.ofSeconds(3));
    }

    public List<EndpointInfo> getActiveEndpoints(Long appId) {
        return webClient.get()
                .uri(baseUrl + "/api/v1/apps/{id}/endpoints", appId)
                .retrieve()
                .bodyToFlux(EndpointInfo.class)
                .collectList()
                .block(Duration.ofSeconds(3))
                .stream()
                .filter(EndpointInfo::ativo)
                .toList();
    }
}
