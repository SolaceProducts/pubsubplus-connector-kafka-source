package com.solace.connector.kafka.connect.source;

import com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor;
import com.solacesystems.jcsmp.JCSMPException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SolaceSourceTaskTest {
	private SolaceSourceTask solaceSourceTask;

	@BeforeEach
	void setUp() {
		solaceSourceTask = new SolaceSourceTask();
	}

	@AfterEach
	void tearDown() {
		solaceSourceTask.stop();
	}

	@Test
	public void testNoProvidedMessageProcessor() {
		Map<String, String> props = Collections.emptyMap();
		ConnectException thrown = Assertions.assertThrows(ConnectException.class, () -> solaceSourceTask.start(props));
		assertThat(thrown.getMessage(), containsString("Encountered exception in creating the message processor."));
		assertThat(thrown.getCause(), instanceOf(KafkaException.class));
		assertThat(thrown.getCause().getMessage(), containsString(
				"Could not find a public no-argument constructor for " + SolMessageProcessorIF.class.getName()));
	}

	@Test
	public void testFailSessionConnect() {
		Map<String, String> props = new HashMap<>();
		props.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR, SolSampleSimpleMessageProcessor.class.getName());

		ConnectException thrown = Assertions.assertThrows(ConnectException.class, () -> solaceSourceTask.start(props));
		assertThat(thrown.getMessage(), containsString("Failed to create JCSMPSession"));
		assertThat(thrown.getCause(), instanceOf(JCSMPException.class));
		assertThat(thrown.getCause().getMessage(), containsString("Null value was passed in for property (host)"));
	}
}
