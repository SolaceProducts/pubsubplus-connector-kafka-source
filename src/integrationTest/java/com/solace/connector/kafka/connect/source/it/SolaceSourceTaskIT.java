package com.solace.connector.kafka.connect.source.it;

import com.solace.connector.kafka.connect.source.SolaceSourceConstants;
import com.solace.connector.kafka.connect.source.SolaceSourceTask;
import com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class SolaceSourceTaskIT {
	private SolaceSourceTask solaceSourceTask;
	private JCSMPProperties jcsmpProperties;

	@Container
	private static final PubSubPlusContainer PUB_SUB_PLUS_CONTAINER = new PubSubPlusContainer();

	@BeforeEach
	void setUp() {
		solaceSourceTask = new SolaceSourceTask();
		jcsmpProperties = new JCSMPProperties();
		jcsmpProperties.setProperty(JCSMPProperties.HOST, PUB_SUB_PLUS_CONTAINER.getOrigin(PubSubPlusContainer.Port.SMF));
		jcsmpProperties.setProperty(JCSMPProperties.USERNAME, "default");
		jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, "default");
	}

	@AfterEach
	void tearDown() {
		solaceSourceTask.stop();
	}

	@Test
	public void testFailTopicListenerInit() {
		String topicName = RandomStringUtils.randomAlphanumeric(100);
		Map<String, String> props = new HashMap<>();
		props.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR, SolSampleSimpleMessageProcessor.class.getName());
		props.put(SolaceSourceConstants.SOL_HOST, jcsmpProperties.getStringProperty(JCSMPProperties.HOST));
		props.put(SolaceSourceConstants.SOL_VPN_NAME, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
		props.put(SolaceSourceConstants.SOL_USERNAME, jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
		props.put(SolaceSourceConstants.SOL_PASSWORD, jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
		props.put(SolaceSourceConstants.SOL_TOPICS, String.join(",", topicName, topicName));

		ConnectException thrown = Assertions.assertThrows(ConnectException.class, () -> solaceSourceTask.start(props));
		assertThat(thrown.getMessage(), containsString("Failed to start topic consumer"));
		assertThat(thrown.getCause(), instanceOf(JCSMPErrorResponseException.class));
		assertEquals(JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT,
				((JCSMPErrorResponseException)thrown.getCause()).getSubcodeEx());
	}

	@Test
	public void testFailQueueConsumerInit() {
		Map<String, String> props = new HashMap<>();
		props.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR, SolSampleSimpleMessageProcessor.class.getName());
		props.put(SolaceSourceConstants.SOL_HOST, jcsmpProperties.getStringProperty(JCSMPProperties.HOST));
		props.put(SolaceSourceConstants.SOL_VPN_NAME, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
		props.put(SolaceSourceConstants.SOL_USERNAME, jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
		props.put(SolaceSourceConstants.SOL_PASSWORD, jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
		props.put(SolaceSourceConstants.SOL_QUEUE, RandomStringUtils.randomAlphanumeric(10));

		ConnectException thrown = Assertions.assertThrows(ConnectException.class, () -> solaceSourceTask.start(props));
		assertThat(thrown.getMessage(), containsString("Failed to start queue consumer"));
		assertThat(thrown.getCause(), instanceOf(JCSMPErrorResponseException.class));
		assertEquals(JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME,
				((JCSMPErrorResponseException)thrown.getCause()).getSubcodeEx());
	}
}
