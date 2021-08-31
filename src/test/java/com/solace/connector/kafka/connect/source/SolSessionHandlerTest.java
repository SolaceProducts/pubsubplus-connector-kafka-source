package com.solace.connector.kafka.connect.source;

import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SolSessionHandlerTest {
	@ParameterizedTest
	@EnumSource(SolacePasswordProperties.class)
	public void testConfigurePasswords(SolacePasswordProperties property) {
		Map<String, String> properties = new HashMap<>();
		properties.put(property.getConnectorProperty(), RandomStringUtils.randomAlphanumeric(30));
		SolSessionHandler sessionHandler = new SolSessionHandler(new SolaceSourceConnectorConfig(properties));
		sessionHandler.configureSession();
		assertEquals(properties.get(property.getConnectorProperty()),
				sessionHandler.properties.getStringProperty(property.getJcsmpProperty()));
	}

	private enum SolacePasswordProperties {
		PASSWORD(SolaceSourceConstants.SOL_PASSWORD, JCSMPProperties.PASSWORD),
		SSL_KEY_STORE_PASSWORD(SolaceSourceConstants.SOL_SSL_KEY_STORE_PASSWORD,
				JCSMPProperties.SSL_KEY_STORE_PASSWORD),
		SSL_PRIVATE_KEY_PASSWORD(SolaceSourceConstants.SOL_SSL_PRIVATE_KEY_PASSWORD,
				JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD),
		SSL_TRUST_STORE_PASSWORD(SolaceSourceConstants.SOL_SSL_TRUST_STORE_PASSWORD,
				JCSMPProperties.SSL_TRUST_STORE_PASSWORD);


		private final String connectorProperty;
		private final String jcsmpProperty;

		SolacePasswordProperties(String connectorProperty, String jcsmpProperty) {
			this.connectorProperty = connectorProperty;
			this.jcsmpProperty = jcsmpProperty;
		}

		public String getConnectorProperty() {
			return connectorProperty;
		}

		public String getJcsmpProperty() {
			return jcsmpProperty;
		}
	}
}
