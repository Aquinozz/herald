package com.aquinozz.herald.endpointservice.service;

import com.aquinozz.herald.endpointservice.dtos.AppRequest;
import com.aquinozz.herald.endpointservice.dtos.AppResponse;
import com.aquinozz.herald.endpointservice.model.App;
import com.aquinozz.herald.endpointservice.repository.AppRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;

@Service
@RequiredArgsConstructor
public class AppService {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final AppRepository appRepository;

    public AppResponse criar(AppRequest request) {
        App app = new App();
        app.setNome(request.nome());
        app.setApiKey(gerarChave());
        app.setSecretHmac(gerarChave());
        return AppResponse.from(appRepository.save(app));
    }

    public List<AppResponse> listar() {
        return appRepository.findAll().stream().map(AppResponse::from).toList();
    }

    public AppResponse buscar(Long id) {
        return AppResponse.from(buscarApp(id));
    }

    public App buscarApp(Long id) {
        return appRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "App não encontrado"));
    }

    private static String gerarChave() {
        byte[] chave = new byte[32];
        SECURE_RANDOM.nextBytes(chave);
        return HexFormat.of().formatHex(chave);
    }
}
