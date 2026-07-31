package com.aquinozz.herald.endpointservice.repository;

import com.aquinozz.herald.endpointservice.model.Endpoint;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface EndpointRepository extends JpaRepository<Endpoint, Long> {

    List<Endpoint> findByAppId(Long appId);
}
