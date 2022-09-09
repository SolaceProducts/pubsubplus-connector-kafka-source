package com.solace.connector.kafka.connect.source;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class SolaceSourceConnectorConfigTest {
	@ParameterizedTest
	@ValueSource(strings = {
			SolaceSourceConstants.SOL_PASSWORD,
			SolaceSourceConstants.SOL_SSL_KEY_STORE_PASSWORD,
			SolaceSourceConstants.SOL_SSL_PRIVATE_KEY_PASSWORD,
			SolaceSourceConstants.SOL_SSL_TRUST_STORE_PASSWORD
	})
	public void testPasswordsObfuscation(String property) {
		Map<String, String> properties = new HashMap<>();
		properties.put(property, RandomStringUtils.randomAlphanumeric(30));
		SolaceSourceConnectorConfig config = new SolaceSourceConnectorConfig(properties);
		Password password = config.getPassword(property);
		assertEquals(Password.HIDDEN, password.toString());
		assertEquals(properties.get(property), password.value());
	}
}
