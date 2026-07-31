package com.aquinozz.herald.endpointservice.service;

import com.aquinozz.herald.endpointservice.dtos.EndpointRequest;
import com.aquinozz.herald.endpointservice.dtos.EndpointResponse;
import com.aquinozz.herald.endpointservice.model.App;
import com.aquinozz.herald.endpointservice.model.Endpoint;
import com.aquinozz.herald.endpointservice.repository.EndpointRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@Service
@RequiredArgsConstructor
public class EndpointService {

    private final EndpointRepository endpointRepository;
    private final AppService appService;

    public EndpointResponse adicionar(Long appId, EndpointRequest request) {
        App app = appService.buscarApp(appId);
        Endpoint endpoint = new Endpoint();
        endpoint.setUrl(request.url());
        endpoint.setAtivo(true);
        endpoint.setApp(app);
        return EndpointResponse.from(endpointRepository.save(endpoint));
    }

    public List<EndpointResponse> listar(Long appId) {
        appService.buscarApp(appId);
        return endpointRepository.findByAppId(appId).stream().map(EndpointResponse::from).toList();
    }

    public EndpointResponse alternarStatus(Long id, boolean ativo) {
        Endpoint endpoint = endpointRepository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Endpoint não encontrado"));
        endpoint.setAtivo(ativo);
        return EndpointResponse.from(endpointRepository.save(endpoint));
    }

    public void deletar(Long id) {
        if (!endpointRepository.existsById(id)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Endpoint não encontrado");
        }
        endpointRepository.deleteById(id);
    }
}
