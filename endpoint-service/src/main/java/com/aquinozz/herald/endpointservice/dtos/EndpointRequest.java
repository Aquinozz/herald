package com.aquinozz.herald.endpointservice.dtos;

import jakarta.validation.constraints.NotBlank;
import org.hibernate.validator.constraints.URL;

public record EndpointRequest(
        @NotBlank(message = "url é obrigatória")
        @URL(message = "url inválida")
        String url) {
}
