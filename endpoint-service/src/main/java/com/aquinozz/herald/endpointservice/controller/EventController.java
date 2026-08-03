package com.aquinozz.herald.endpointservice.controller;

import com.aquinozz.herald.endpointservice.dtos.EventRequest;
import com.aquinozz.herald.endpointservice.dtos.EventResponse;
import com.aquinozz.herald.endpointservice.service.AppService;
import com.aquinozz.herald.endpointservice.service.EventPublisherService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class EventController {

    private final EventPublisherService eventPublisherService;
    private final AppService appService;

    @PostMapping("/apps/{appId}/events")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EventResponse publicar(@PathVariable Long appId, @Valid @RequestBody EventRequest request) {
        appService.buscarApp(appId);
        String eventId = eventPublisherService.publish(appId, request);
        return new EventResponse(eventId, EventResponse.STATUS_RECEIVED);
    }
}