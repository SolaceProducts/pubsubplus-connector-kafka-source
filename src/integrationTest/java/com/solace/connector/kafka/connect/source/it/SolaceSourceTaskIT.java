package com.solace.connector.kafka.connect.source.it;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.solace.connector.kafka.connect.source.SolMessageProcessorIF;
import com.solace.connector.kafka.connect.source.SolaceSourceConstants;
import com.solace.connector.kafka.connect.source.SolaceSourceTask;
import com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension;
import com.solace.test.integration.junit.jupiter.extension.LogCaptorExtension.LogCaptor;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.io.BufferedReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(LogCaptorExtension.class)
@ExtendWith(PubSubPlusExtension.class)
public class SolaceSourceTaskIT {
	private SolaceSourceTask solaceSourceTask;
	private Map<String, String> connectorProperties;

	private static final Logger logger = LoggerFactory.getLogger(SolaceSourceTaskIT.class);

	@BeforeEach
	void setUp(JCSMPProperties jcsmpProperties) {
		solaceSourceTask = new SolaceSourceTask();

		connectorProperties = new HashMap<>();
		connectorProperties.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR, TestConstants.CONN_MSGPROC_CLASS);
		connectorProperties.put(SolaceSourceConstants.SOL_HOST, jcsmpProperties.getStringProperty(JCSMPProperties.HOST));
		connectorProperties.put(SolaceSourceConstants.SOL_VPN_NAME, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
		connectorProperties.put(SolaceSourceConstants.SOL_USERNAME, jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
		connectorProperties.put(SolaceSourceConstants.SOL_PASSWORD, jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
	}

	@AfterEach
	void tearDown() {
		solaceSourceTask.stop();
	}

	@Test
	public void testFailTopicListenerInit() {
		String topicName = RandomStringUtils.randomAlphanumeric(100);
		connectorProperties.put(SolaceSourceConstants.SOL_TOPICS, String.join(",", topicName, topicName));

		assertThatThrownBy(() -> solaceSourceTask.start(connectorProperties))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Failed to start topic consumer")
				.cause()
				.asInstanceOf(InstanceOfAssertFactories.throwable(JCSMPErrorResponseException.class))
				.extracting(JCSMPErrorResponseException::getSubcodeEx)
				.isEqualTo(JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT);
	}

	@Test
	public void testFailQueueConsumerInit() {
		connectorProperties.put(SolaceSourceConstants.SOL_QUEUE, RandomStringUtils.randomAlphanumeric(10));

		assertThatThrownBy(() -> solaceSourceTask.start(connectorProperties))
				.isInstanceOf(ConnectException.class)
				.hasMessageContaining("Failed to start queue consumer")
				.cause()
				.asInstanceOf(InstanceOfAssertFactories.throwable(JCSMPErrorResponseException.class))
				.extracting(JCSMPErrorResponseException::getSubcodeEx)
				.isEqualTo(JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME);
	}

	@ParameterizedTest(name = "[{index}] ignoreMessageProcessorError={0}")
	@ValueSource(booleans = { true, false })
	public void testMessageProcessorError(boolean ignoreMessageProcessorError,
										  JCSMPSession jcsmpSession,
										  SempV2Api sempV2Api,
										  Queue queue,
										  @ExecSvc ExecutorService executorService,
										  @LogCaptor(SolaceSourceTask.class) BufferedReader logReader) throws Exception {
		String vpnName = connectorProperties.get(SolaceSourceConstants.SOL_VPN_NAME);

		connectorProperties.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR, BadMessageProcessor.class.getName());
		connectorProperties.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_IGNORE_ERROR, Boolean.toString(ignoreMessageProcessorError));
		connectorProperties.put(SolaceSourceConstants.SOL_QUEUE, queue.getName());
		solaceSourceTask.start(connectorProperties);

		XMLMessageProducer messageProducer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			@Override
			public void responseReceivedEx(Object o) {
				// not used
			}

			@Override
			public void handleErrorEx(Object o, JCSMPException e, long l) {
				// not used
			}
		});

		try {
			TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			message.setText("Test payload");
			messageProducer.send(message, queue);
		} finally {
			messageProducer.close();
		}

		await(String.format("queue %s to deliver messages", queue.getName()))
				.atMost(30, SECONDS)
				.pollInterval(1, SECONDS)
				.until(() -> {
					logger.info("Waiting for queue {} to deliver messages", queue.getName());
					return sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null)
							.getData().getTxUnackedMsgCount() > 0;
				});

		if (ignoreMessageProcessorError) {
			Future<?> future = executorService.submit((Callable<Void>) () -> {
				String logLine;
				do {
					logger.info("Waiting for error log message");
					logLine = logReader.readLine();
				} while (!logLine.contains("Encountered exception in message processing"));
				return null;
			});
			assertThat(solaceSourceTask.poll()).isEmpty();
			future.get(30, TimeUnit.SECONDS);
			solaceSourceTask.commit();
			await(String.format("queue %s to be empty", queue.getName()))
					.atMost(30, SECONDS)
					.pollInterval(1, SECONDS)
					.until(() -> {
						logger.info("Waiting for queue {} to be empty", queue.getName());
						return sempV2Api.monitor()
								.getMsgVpnQueueMsgs(vpnName, queue.getName(), 1, null, null, null)
								.getData()
								.isEmpty();
					});
		} else {
			assertThatThrownBy(() -> solaceSourceTask.poll())
					.isInstanceOf(ConnectException.class)
					.hasMessageContaining("Encountered exception in message processing")
					.hasCause(BadMessageProcessor.TEST_EXCEPTION);
			solaceSourceTask.commit();
			Thread.sleep(Duration.ofSeconds(5).toMillis());
			assertThat(sempV2Api.monitor().getMsgVpnQueue(vpnName, queue.getName(), null)
					.getData()
					.getTxUnackedMsgCount())
					.isEqualTo(1);
		}
	}

	public static class BadMessageProcessor extends SolSampleSimpleMessageProcessor {
		static final RuntimeException TEST_EXCEPTION = new RuntimeException("Some processing failure");

		@Override
		public SolMessageProcessorIF process(String skey, BytesXMLMessage message) {
			throw TEST_EXCEPTION;
		}
	}
}
