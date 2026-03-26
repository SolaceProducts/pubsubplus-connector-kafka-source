package com.solace.connector.kafka.connect.source.it;

import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_MAP_SOLACE_STANDARD_PROPERTIES;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_MAP_USER_PROPERTIES;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.solace.connector.kafka.connect.source.SolaceSourceConstants;
import com.solace.connector.kafka.connect.source.it.util.extensions.KafkaArgumentsProvider.KafkaArgumentSource;
import com.solace.connector.kafka.connect.source.it.util.extensions.KafkaArgumentsProvider.KafkaContext;
import com.solace.connector.kafka.connect.source.it.util.extensions.KafkaArgumentsProvider.ResetKafkaContextAfterEach;
import com.solace.connector.kafka.connect.source.it.util.extensions.pubsubplus.pubsubplus.NetworkPubSubPlusContainerProvider;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.impl.AbstractDestination;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(ResetKafkaContextAfterEach.class)
public class SourceConnectorIT implements TestConstants {

  private Properties connectorProps;
  private static final Logger LOG = LoggerFactory.getLogger(SourceConnectorIT.class);
  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  static TestSolaceProducer solaceProducer;

  ////////////////////////////////////////////////////
  // Main setup/teardown

  @BeforeAll
  static void setUp(JCSMPSession jcsmpSession, SempV2Api sempV2Api) throws Exception {
    solaceProducer = new TestSolaceProducer(jcsmpSession, sempV2Api);
    solaceProducer.start();
  }

  @BeforeEach
  public void beforeEach(JCSMPProperties jcsmpProperties) {
    connectorProps = new Properties();
    connectorProps.setProperty(SolaceSourceConstants.SOL_HOST, String.format("tcp://%s:55555", NetworkPubSubPlusContainerProvider.DOCKER_NET_PUBSUB_ALIAS));
    connectorProps.setProperty(SolaceSourceConstants.SOL_USERNAME, jcsmpProperties.getStringProperty(JCSMPProperties.USERNAME));
    connectorProps.setProperty(SolaceSourceConstants.SOL_PASSWORD, jcsmpProperties.getStringProperty(JCSMPProperties.PASSWORD));
    connectorProps.setProperty(SolaceSourceConstants.SOL_VPN_NAME, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME));
  }

  @AfterAll
  static void cleanUp() {
    solaceProducer.close();
  }

  ////////////////////////////////////////////////////
  // Test types

  private void messageToKafkaTest(Message msg, AbstractDestination destination, String expectedValue, Object expectedKey, KafkaContext kafkaContext) {
    try {
      // Send Solace message
      if (destination instanceof Topic) {
        solaceProducer.sendMessageToTopic((Topic) destination, msg);
      } else {
        solaceProducer.sendMessageToQueue((Queue) destination, msg);
      }
      // Wait for Kafka to report message
      ConsumerRecords<Object, Object> records = kafkaContext.getConsumer().poll(Duration.ofSeconds(5));
      assertEquals(1, records.count());
      ConsumerRecord<Object, Object> record = records.iterator().next();
      // Evaluate message
      assertNotNull(record);
      LOG.info("Kafka message received - Key={}, Value={}", record.key(), record.value());
      assertEquals(expectedValue, record.value());
      // Check key
      if (expectedKey == null) {
        assert (record.key() == null);
      } else {
        assert (record.key() instanceof ByteBuffer);
        ByteBuffer bb = (ByteBuffer) record.key();
        byte[] b = new byte[bb.remaining()];
        bb.get(b);
        if (expectedKey instanceof String) {
          assert (Arrays.equals(b, ((String) expectedKey).getBytes()));
        } else {
          assert (Arrays.equals(b, (byte[]) expectedKey));
        }
      }

      if ("true".equalsIgnoreCase(connectorProps.getProperty(SOL_MESSAGE_PROCESSOR_MAP_USER_PROPERTIES))
          || "true".equalsIgnoreCase(connectorProps.getProperty(SOL_MESSAGE_PROCESSOR_MAP_SOLACE_STANDARD_PROPERTIES))) {

        //verify user properties
        final SDTMap solUserProperties = msg.getProperties();
        final RecordHeaders recordHeaders = new RecordHeaders(record.headers().toArray());
        if (solUserProperties != null && !solUserProperties.keySet().isEmpty()) {
          LOG.info("Headers: {}", recordHeaders);
          solUserProperties.keySet().forEach(key -> assertNotNull(recordHeaders.remove(key))); //Removes header
        }

        //Any remaining headers must be solace standard headers
        if (recordHeaders.toArray().length > 0) {
          LOG.info("Headers: {}", recordHeaders);
          recordHeaders.iterator().forEachRemaining(header -> assertTrue(header.key().startsWith("solace_")));
        }
      } else {
        assertThat(record.headers().toArray()).isEmpty();
      }
    } catch (JCSMPException e1) {
      e1.printStackTrace();
    }
  }

  ////////////////////////////////////////////////////
  // Scenarios

  @DisplayName("Solace connector SimpleMessageProcessor tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorSimpleMessageProcessorTests {

    ////////////////////////////////////////////////////
    // Scenarios

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "false");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "false");
      connectorProps.setProperty("sol.topics", "TestTopic1/SubTopic");
      connectorProps.setProperty("sol.username", "test");
      connectorProps.setProperty("sol.password", "test");
    }

    @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("1-Hello TextMessageToTopicTest world!");
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "1-Hello TextMessageToTopicTest world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(new byte[] { '2', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'T',
          'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "2-Hello Topic world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest(KafkaContext kafkaContext)
        throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { '3', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd',
          ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "3-Hello attached world!", null, kafkaContext);
    }

    @DisplayName("TextMessage-Queue-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("4-Hello TextmessageToKafkaTest world!");
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "4-Hello TextmessageToKafkaTest world!", null, kafkaContext);
    }

    @DisplayName("BytesMessage-Queue-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerBytesmessageToKafkaTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(new byte[] { '5', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'Q',
          'u', 'e', 'u', 'e', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "5-Hello Queue world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Queue-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToQueueTest(KafkaContext kafkaContext)
        throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { '6', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd',
          ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "6-Hello attached world!", null, kafkaContext);
    }

  }

  //////////////////////////////////////////////////////////////////////////////////////////

  @DisplayName("Solace connector SolSampleKeyedMessageProcessor-NONE tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorNoneKeyedMessageProcessorTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "false");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "false");
      connectorProps.setProperty("sol.kafka_message_key", "NONE");
      connectorProps.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
    }

    @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest1 world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'T', 'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello Topic world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest(KafkaContext kafkaContext)
        throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd', ' ', 'w',
          'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello attached world!", null, kafkaContext);
    }

    @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello TextmessageToKafkaTest world!", null, kafkaContext);
    }

    @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerBytesmessageToKafkaTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'Q', 'u', 'e', 'u', 'e', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello Queue world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToQueueTest(KafkaContext kafkaContext)
        throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd', ' ', 'w',
          'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello attached world!", null, kafkaContext);
    }

  }

  //////////////////////////////////////////////////////////////////////////////////////////

  @DisplayName("Solace connector SolSampleKeyedMessageProcessor-DESTINATION tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorDestinationKeyedMessageProcessorTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "false");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "false");
      connectorProps.setProperty("sol.kafka_message_key", "DESTINATION");
      connectorProps.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
    }

    @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest1 world!", "TestTopic1/SubTopic", kafkaContext);
    }

    @DisplayName("TextMessage-Topic-wildcard-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest2(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest2 world!");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic2/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest2 world!", "TestTopic2/SubTopic", kafkaContext);
    }

    @DisplayName("TextMessage-Topic-multi-level-wildcard-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest3(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest3 world!");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic3/SubTopic/SubSubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest3 world!", "TestTopic3/SubTopic/SubSubTopic", kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'T', 'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello Topic world!", "TestTopic1/SubTopic", kafkaContext);
    }

    @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello TextmessageToKafkaTest world!", SOL_QUEUE, kafkaContext);
    }

    @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerBytesmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'Q', 'u', 'e', 'u', 'e', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello Queue world!", SOL_QUEUE, kafkaContext);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////

  @DisplayName("Solace connector SolSampleKeyedMessageProcessor-CORRELATION_ID tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorCorrelationIdKeyedMessageProcessorTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "false");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "false");
      connectorProps.setProperty("sol.kafka_message_key", "CORRELATION_ID");
      connectorProps.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
    }

    @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
      msg.setCorrelationId("test");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest1 world!", "test", kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'T', 'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setCorrelationId("test2");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello Topic world!", "test2", kafkaContext);
    }

    @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
      msg.setCorrelationId("test3");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello TextmessageToKafkaTest world!", "test3", kafkaContext);
    }

    @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerBytesmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'Q', 'u', 'e', 'u', 'e', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setCorrelationId("test4");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello Queue world!", "test4", kafkaContext);
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////

  @DisplayName("Solace connector SolSampleKeyedMessageProcessor-CORRELATION_ID_AS_BYTES tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorCorrelationIdAsBytesKeyedMessageProcessorTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "false");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "false");
      connectorProps.setProperty("sol.kafka_message_key", "CORRELATION_ID_AS_BYTES");
      connectorProps.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
      connectorProps.setProperty("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    }

    @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
      msg.setCorrelationId(new String(new byte[] { 1, 2, 3, 4 }));
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest1 world!", new String(new byte[] { 1, 2, 3, 4 }), kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'T', 'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setCorrelationId("test2");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello Topic world!", "test2", kafkaContext);
    }

    @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
      msg.setCorrelationId("test3");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello TextmessageToKafkaTest world!", "test3", kafkaContext);
    }

    @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerBytesmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'Q', 'u', 'e', 'u', 'e', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setCorrelationId("test4");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello Queue world!", "test4", kafkaContext);
    }
  }

  ////////////////////////////////////////////////////
  // Scenarios

  @DisplayName("Solace connector SharedSubscriptions tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorSharedSubscriptionsTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "true");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "true");
      connectorProps.setProperty("sol.topics", "#share/group1/TestTopic1/SubTopic");
      connectorProps.setProperty("tasks.max", "5");
    }

    @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest world!");
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) throws SDTException {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'T', 'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      msg.setProperties(getTestUserProperties());
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello Topic world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd', ' ', 'w',
          'o', 'r', 'l', 'd', '!' });
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello attached world!", null, kafkaContext);
    }

  }

  @DisplayName("Solace connector provisioning tests")
  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorProvisioningTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
    }

    @ParameterizedTest
    @KafkaArgumentSource
    void testFailPubSubConnection(KafkaContext kafkaContext) {
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.message_processor.map_user_properties", "true");
      connectorProps.setProperty("sol.message_processor.map_solace_standard_properties", "true");
      connectorProps.setProperty("sol.vpn_name", RandomStringUtils.randomAlphanumeric(10));
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps, true);
      AtomicReference<JsonObject> connectorStatus = new AtomicReference<>(new JsonObject());
      await("connector to fail with VPN error")
          .atMost(1, MINUTES)
          .untilAsserted(() -> {
            JsonObject status = kafkaContext.getSolaceConnectorDeployment().getConnectorStatus();
            connectorStatus.set(status);
            JsonObject taskStatus = status.getAsJsonArray("tasks").get(0).getAsJsonObject();
            assertThat(taskStatus.get("state").getAsString())
                .as("Connector task not in FAILED state: %s", GSON.toJson(status))
                .isEqualTo("FAILED");
          });
      assertThat(connectorStatus.get().getAsJsonArray("tasks").get(0).getAsJsonObject()
          .get("trace").getAsString()).contains("Message VPN Not Allowed");
    }
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class SolaceConnectorAcknowledgmentTests {

    @BeforeEach
    void setUp() throws Exception {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.queue", SOL_QUEUE);
    }

    /**
     * This test verifies that messages are NOT ACKed when Kafka producer send fails.
     */
    @DisplayName("Kafka Send Failure Does Not ACK Message")
    @ParameterizedTest
    @KafkaArgumentSource
    void testKafkaSendFailureDoesNotAckMessage(KafkaContext kafkaContext,
                                               SempV2Api sempV2Api,
                                               JCSMPSession jcsmpSession) throws Exception {
      String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

      assertThat(sempV2Api.monitor().getMsgVpnQueue(vpnName, SOL_QUEUE, null).getData()
          .getMaxRedeliveryExceededDiscardedMsgCount())
          .as("Test Setup Error: Expected no messages to be discarded on queue")
          .isZero();

      // Configure connector with tiny max.request.size to force RecordTooLargeException
      // This causes Kafka producer send to fail, triggering the bug scenario
      connectorProps.setProperty("producer.override.max.request.size", "10");

      // Start connector in REAL Kafka Connect framework
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);

      // Send large message to Solace (>10 bytes) to trigger Kafka send failure
      TextMessage largeMsg = solaceProducer.createTextMessage(RandomStringUtils.randomAlphanumeric(200));
      solaceProducer.sendMessageToQueue(solaceProducer.defineQueue(SOL_QUEUE), largeMsg);

      // Wait for message to be delivered to connector (appears as unacked)
      AtomicReference<JsonObject> connectorStatus = new AtomicReference<>(new JsonObject());
      await("connector to fail with RecordTooLargeException")
          .atMost(1, MINUTES)
          .untilAsserted(() -> {
            JsonObject status = kafkaContext.getSolaceConnectorDeployment().getConnectorStatus();
            connectorStatus.set(status);
            JsonObject taskStatus = status.getAsJsonArray("tasks").get(0).getAsJsonObject();
            assertThat(taskStatus.get("state").getAsString())
                .as("Connector task not in FAILED state: %s", GSON.toJson(status))
                .isEqualTo("FAILED");
          });
      assertThat(connectorStatus.get().getAsJsonArray("tasks").get(0).getAsJsonObject()
              .get("trace").getAsString()).contains("RecordTooLargeException");

      // Attach a new polling flow, because the broker needs something else to connect to
      // the queue for unack'd messages after a flow shutdown to be processed...
      ConsumerFlowProperties flowProperties = new ConsumerFlowProperties();
      flowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(SOL_QUEUE));
      flowProperties.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
      flowProperties.setStartState(true);
      FlowReceiver probeFlow = jcsmpSession.createFlow(null, flowProperties, null);
      try {
        await(String.format("message to be rejected on queue %s", SOL_QUEUE))
                .atMost(5, MINUTES)
                .pollInterval(1, SECONDS)
                .until(() -> {
                  LOG.info("Waiting for message to be rejected on queue {}", SOL_QUEUE);
                  long count = sempV2Api.monitor()
                          .getMsgVpnQueue(vpnName, SOL_QUEUE, null)
                          .getData()
                          .getMaxRedeliveryExceededDiscardedMsgCount();
                  return count > 0;
                });

        // just a sanity check to ensure the probe didnt actually get anything...
        assertThat(probeFlow.receiveNoWait())
            .as("Expected no messages to be received on flow, but got one")
            .isNull();
      } finally {
        probeFlow.close();
      }

      assertThat(kafkaContext.getConsumer().poll(Duration.ofSeconds(1)))
          .as("Expected no messages to be received by Kafka consumer")
          .isEmpty();
    }

    /**
     * This test verifies that messages ARE ACKed when Kafka producer send fails
     * with errors.tolerance=all. Framework calls commitRecord(record, null) in this case,
     * and the connector should ACK to prevent infinite redelivery loops.
     */
    @DisplayName("Kafka Send Failure With Errors Tolerance All ACKs Message")
    @ParameterizedTest
    @KafkaArgumentSource
    void testKafkaSendFailureWithErrorsToleranceAllAcksMessage(
        KafkaContext kafkaContext,
        SempV2Api sempV2Api,
        JCSMPSession jcsmpSession) throws Exception {
      String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

      // Configure connector with errors.tolerance=all and tiny max.request.size
      // errors.tolerance=all causes framework to call commitRecord(record, null) on send failure
      // This tests that we correctly ACK the message to prevent infinite redelivery
      connectorProps.setProperty("errors.tolerance", "all");
      connectorProps.setProperty("producer.override.max.request.size", "150");

      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);

      for (int msgIdx = 1; msgIdx <= 5; msgIdx++) {
        int finalMsgIdx = msgIdx;

        TextMessage largeMsg = solaceProducer.createTextMessage(
            msgIdx % 2 == 0
                // Send large message to trigger Kafka send failure
                ? RandomStringUtils.randomAlphanumeric(200)
                // Send small message to ensure that success cases still work
                : Integer.toString(msgIdx));
        solaceProducer.sendMessageToQueue(solaceProducer.defineQueue(SOL_QUEUE), largeMsg);

        // Wait for message to be delivered to connector
        await(String.format("message to be delivered on queue %s", SOL_QUEUE))
                .atMost(30, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> {
                  LOG.info("Waiting for message to be delivered to connector on queue {}", SOL_QUEUE);
                  List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
                          .getMsgVpnQueueTxFlows(vpnName, SOL_QUEUE, null, null, null, null)
                          .getData();
                  return !txFlows.isEmpty() && txFlows.get(0).getAckedMsgCount() >= finalMsgIdx;
                });

        // Give connector time to attempt send and call commitRecord with null metadata
        Thread.sleep(Duration.ofSeconds(5).toMillis());

        LOG.info("Verify connector is still RUNNING (not FAILED)");
        assertThat(kafkaContext.getSolaceConnectorDeployment().getConnectorStatus())
            .extracting(status -> status.getAsJsonArray("tasks").get(0).getAsJsonObject())
            .extracting(status -> status.get("state").getAsString())
            .as("Expected connector to stay RUNNING with errors.tolerance=all")
            .isEqualTo("RUNNING");

        LOG.info("Verify message WAS ACKed (queue becomes empty)");
        // With errors.tolerance=all, framework calls commitRecord(record, null)
        // Connector should ACK to prevent infinite redelivery loop
        assertThat(sempV2Api.monitor()
            .getMsgVpnQueueMsgs(vpnName, SOL_QUEUE, finalMsgIdx, null, null, null)
            .getData())
            .as("Expected queue %s to be empty", SOL_QUEUE)
            .isEmpty();
      }

      LOG.info("Verifying that small(good) records still make it through correctly");
      assertThat(kafkaContext.getConsumer().poll(Duration.ofSeconds(5)))
          .extracting(ConsumerRecord::value)
          .containsExactly("1", "3", "5");
    }

    /**
     * This test verifies that message acknowledgment works correctly when Kafka Connect
     * transformations are applied. The framework creates new SourceRecord instances during
     * transformation but passes the original preTransformRecord to commitRecord(), which
     * relies on IdentityHashMap in MessageTracker.
     */
    @DisplayName("Transformation Does Not Prevent Message Acknowledgment")
    @ParameterizedTest
    @KafkaArgumentSource
    void testTransformationDoesNotPreventMessageAcknowledgment(
        KafkaContext kafkaContext,
        SempV2Api sempV2Api,
        JCSMPSession jcsmpSession) throws Exception {

      // SETUP: Get VPN name for SEMP API calls
      String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

      // SETUP: Configure connector with InsertHeader transformation
      // This transformation creates new SourceRecord instances, but framework
      // passes original preTransformRecord to commitRecord()
      connectorProps.setProperty("transforms", "insertHeader");
      connectorProps.setProperty("transforms.insertHeader.type",
          "org.apache.kafka.connect.transforms.InsertHeader");
      connectorProps.setProperty("transforms.insertHeader.header", "transformed_by");
      connectorProps.setProperty("transforms.insertHeader.value.literal", "solace-connector");

      // STEP 1: Start connector with transformation
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);

      // STEP 2: Send test messages to Solace queue
      int messageCount = 3;
      for (int i = 1; i <= messageCount; i++) {
        TextMessage msg = solaceProducer.createTextMessage("test-message-" + i);
        solaceProducer.sendMessageToQueue(solaceProducer.defineQueue(SOL_QUEUE), msg);
      }

      // STEP 3: Wait for messages to be ACKed (ensures Kafka writes are complete)
      LOG.info("Waiting for all messages to be ACKed");
      await("all messages to be ACKed")
          .atMost(30, SECONDS)
          .pollInterval(1, SECONDS)
          .until(() -> {
            LOG.info("Waiting for message to be delivered to connector on queue {}", SOL_QUEUE);
            List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
                .getMsgVpnQueueTxFlows(vpnName, SOL_QUEUE, null, null, null, null)
                .getData();
            return !txFlows.isEmpty() && txFlows.get(0).getAckedMsgCount() >= messageCount;
          });

      // STEP 4: Verify transformed messages appear in Kafka with header
      LOG.info("Verifying transformed messages in Kafka with header");
      ConsumerRecords<Object, Object> records =
          kafkaContext.getConsumer().poll(Duration.ofSeconds(10));

      // STEP 5: Verify each record has the transformation header
      assertThat(records)
          .as("Expected %d messages to be received by Kafka consumer", messageCount)
          .hasSize(messageCount)
          .allSatisfy(sourceRecord -> {
            assertThat(sourceRecord.headers().lastHeader("transformed_by"))
                .as("Expected 'transformed_by' header to be present")
                .isNotNull();

            assertThat(new String(sourceRecord.headers().lastHeader("transformed_by").value()))
                .as("Expected 'transformed_by' header value")
                .isEqualTo("solace-connector");
          });

      assertThat(sempV2Api.monitor()
          .getMsgVpnQueueMsgs(vpnName, SOL_QUEUE, null, null, null, null)
          .getData())
          .as("Expected queue to be empty")
          .isEmpty();

      // STEP 6: Verify connector remains RUNNING
      LOG.info("Verifying connector is still RUNNING");
      assertThat(kafkaContext.getSolaceConnectorDeployment().getConnectorStatus())
          .extracting(status -> status.getAsJsonArray("tasks").get(0).getAsJsonObject())
          .extracting(status -> status.get("state").getAsString())
          .as("Expected connector to stay RUNNING after transformations")
          .isEqualTo("RUNNING");
    }
  }

  private SDTMap getTestUserProperties() throws SDTException {
    final SDTMap solMsgUserProperties = JCSMPFactory.onlyInstance().createMap();
    solMsgUserProperties.putObject("null-value-user-property", null);
    solMsgUserProperties.putBoolean("boolean-user-property", true);
    solMsgUserProperties.putCharacter("char-user-property", 'C');
    solMsgUserProperties.putDouble("double-user-property", 123.4567);
    solMsgUserProperties.putFloat("float-user-property", 000.1f);
    solMsgUserProperties.putInteger("int-user-property", 1);
    solMsgUserProperties.putLong("long-user-property", 10000L);
    solMsgUserProperties.putShort("short-user-property", Short.valueOf("20"));
    solMsgUserProperties.putString("string-user-property", "value1");
    solMsgUserProperties.putObject("bigInteger-user-property", BigInteger.valueOf(123456L));
    solMsgUserProperties.putByte("byte-user-property", "A".getBytes()[0]);
    solMsgUserProperties.putBytes("bytes-user-property", "Hello".getBytes());
    solMsgUserProperties.putByteArray("byteArray-user-property",
        new ByteArray("Hello World".getBytes()));
    solMsgUserProperties.putDestination("topic-user-property",
        JCSMPFactory.onlyInstance().createTopic("testTopic"));
    solMsgUserProperties.putDestination("queue-user-property",
        JCSMPFactory.onlyInstance().createTopic("testQueue"));

    return solMsgUserProperties;
  }
}
