package com.aquinozz.herald.endpointservice.controller;

import com.aquinozz.herald.endpointservice.dtos.EndpointRequest;
import com.aquinozz.herald.endpointservice.dtos.EndpointResponse;
import com.aquinozz.herald.endpointservice.service.EndpointService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class EndpointController {

    private final EndpointService endpointService;

    @PostMapping("/apps/{appId}/endpoints")
    @ResponseStatus(HttpStatus.CREATED)
    public EndpointResponse adicionar(@PathVariable Long appId, @Valid @RequestBody EndpointRequest request) {
        return endpointService.adicionar(appId, request);
    }

    @GetMapping("/apps/{appId}/endpoints")
    public List<EndpointResponse> listar(@PathVariable Long appId) {
        return endpointService.listar(appId);
    }

    @PatchMapping("/endpoints/{id}")
    public EndpointResponse alternarStatus(@PathVariable Long id, @RequestParam boolean ativo) {
        return endpointService.alternarStatus(id, ativo);
    }

    @DeleteMapping("/endpoints/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deletar(@PathVariable Long id) {
        endpointService.deletar(id);
    }
}
