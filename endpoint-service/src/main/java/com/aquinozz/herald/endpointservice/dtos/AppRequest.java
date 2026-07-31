package com.aquinozz.herald.endpointservice.dtos;

import jakarta.validation.constraints.NotBlank;

public record AppRequest(@NotBlank(message = "nome é obrigatório") String nome) {
}
