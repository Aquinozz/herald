package com.aquinozz.herald.webhookdispatcher;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class WebhookDispatcherApplication {

	public static void main(String[] args) {
		SpringApplication.run(WebhookDispatcherApplication.class, args);
	}

}