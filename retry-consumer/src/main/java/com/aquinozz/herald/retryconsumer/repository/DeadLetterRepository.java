package com.aquinozz.herald.retryconsumer.repository;

import com.aquinozz.herald.retryconsumer.model.DeadLetter;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DeadLetterRepository extends MongoRepository<DeadLetter, String> {
}