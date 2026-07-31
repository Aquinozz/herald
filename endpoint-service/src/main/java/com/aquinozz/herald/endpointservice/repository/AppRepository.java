package com.aquinozz.herald.endpointservice.repository;

import com.aquinozz.herald.endpointservice.model.App;
import org.springframework.data.jpa.repository.JpaRepository;

public interface AppRepository extends JpaRepository<App, Long> {
}
