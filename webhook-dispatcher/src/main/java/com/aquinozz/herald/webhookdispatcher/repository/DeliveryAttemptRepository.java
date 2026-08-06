package com.aquinozz.herald.webhookdispatcher.repository;

import com.aquinozz.herald.webhookdispatcher.model.DeliveryAttempt;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;
import java.util.Optional;

public interface DeliveryAttemptRepository extends MongoRepository<DeliveryAttempt, String> {

    List<DeliveryAttempt> findByAppIdOrderByTimestampDesc(Long appId);

    Optional<DeliveryAttempt> findFirstByEventIdAndEndpointIdAndStatus(
            String eventId, Long endpointId, DeliveryAttempt.Status status);
}
