package com.aquinozz.herald.endpointservice.dtos;

import com.aquinozz.herald.endpointservice.model.Endpoint;

public record EndpointResponse(Long id, String url, boolean ativo) {

    public static EndpointResponse from(Endpoint endpoint) {
        return new EndpointResponse(endpoint.getId(), endpoint.getUrl(), endpoint.isAtivo());
    }
}
