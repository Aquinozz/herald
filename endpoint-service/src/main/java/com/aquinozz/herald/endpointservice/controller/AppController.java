package com.aquinozz.herald.endpointservice.controller;

import com.aquinozz.herald.endpointservice.dtos.AppRequest;
import com.aquinozz.herald.endpointservice.dtos.AppResponse;
import com.aquinozz.herald.endpointservice.service.AppService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/apps")
@RequiredArgsConstructor
@Tag(name = "Apps", description = "Cadastro de aplicacoes que receberao webhooks")
public class AppController {

    private final AppService appService;

    @Operation(summary = "Criar app", description = "Cria um app e gera a apiKey e o secretHmac usados no fluxo de webhooks")
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public AppResponse criar(@Valid @RequestBody AppRequest request) {
        return appService.criar(request);
    }

    @Operation(summary = "Listar apps", description = "Retorna todos os apps cadastrados")
    @GetMapping
    public List<AppResponse> listar() {
        return appService.listar();
    }

    @Operation(summary = "Buscar app", description = "Retorna um app pelo id")
    @GetMapping("/{id}")
    public AppResponse buscar(@PathVariable Long id) {
        return appService.buscar(id);
    }
}