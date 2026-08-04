package com.aquinozz.herald.webhookdispatcher.repository;

import com.aquinozz.herald.webhookdispatcher.model.DeliveryAttempt;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface DeliveryAttemptRepository extends MongoRepository<DeliveryAttempt, String> {

    List<DeliveryAttempt> findByAppIdOrderByTimestampDesc(Long appId);
}
