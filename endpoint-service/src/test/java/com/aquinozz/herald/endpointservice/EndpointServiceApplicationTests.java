package com.aquinozz.herald.endpointservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
		"spring.datasource.url=jdbc:h2:mem:testdb",
		"spring.jpa.hibernate.ddl-auto=create-drop"
})
class EndpointServiceApplicationTests {

	@Test
	void contextLoads() {
	}

}
