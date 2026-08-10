package com.aquinozz.herald.endpointservice.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    @Bean
    public OpenAPI heraldOpenAPI(@Value("${herald.openapi.server-url:http://localhost:8080}") String serverUrl) {
        return new OpenAPI()
                .servers(List.of(new Server().url(serverUrl).description("Herald - via gateway")))
                .info(new Info()
                        .title("Herald API")
                        .description("""
                                Motor de entrega de webhooks confiavel.
                                Gerencia apps e endpoints, ingere eventos (retorno 202 imediato)
                                e entrega de forma assincrona com retry exponencial, idempotencia,
                                assinatura HMAC e Dead Letter Queue.
                                Prototipo - acesse aqui: https://github.com/Aquinozz/herald
                                """)
                        .version("1.0.0"));
    }
}