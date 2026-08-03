package com.aquinozz.herald.endpointservice.dtos;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;

public record EventRequest(
        @NotBlank(message = "type é obrigatório")
        String type,
        JsonNode data) {
}
