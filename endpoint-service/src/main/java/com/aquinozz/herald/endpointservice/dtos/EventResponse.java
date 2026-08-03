package com.aquinozz.herald.endpointservice.dtos;

public record EventResponse(String eventId, String status) {

    public static final String STATUS_RECEIVED = "RECEIVED";
}
