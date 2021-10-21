package com.solace.connector.kafka.connect.source.it;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.solace.connector.kafka.connect.source.SolaceSourceConstants;
import com.solace.connector.kafka.connect.source.it.util.extensions.KafkaArgumentsProvider;
import com.solace.connector.kafka.connect.source.it.util.extensions.KafkaArgumentsProvider.KafkaArgumentSource;
import com.solace.connector.kafka.connect.source.it.util.extensions.KafkaArgumentsProvider.KafkaContext;
import com.solace.connector.kafka.connect.source.it.util.extensions.NetworkPubSubPlusExtension;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.impl.AbstractDestination;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ExtendWith(NetworkPubSubPlusExtension.class)
@ExtendWith(KafkaArgumentsProvider.AutoDeleteSolaceConnectorDeploymentAfterEach.class)
public class SourceConnectorIT implements TestConstants {

  private Properties connectorProps;
  private static final Logger LOG = LoggerFactory.getLogger(SourceConnectorIT.class);
  static TestSolaceProducer solaceProducer;

  ////////////////////////////////////////////////////
  // Main setup/teardown

  @BeforeAll
  static void setUp(JCSMPSession jcsmpSession) throws Exception {
    solaceProducer = new TestSolaceProducer(jcsmpSession);
    solaceProducer.start();
  }

  @BeforeEach
  public void beforeEach(JCSMPProperties jcsmpProperties) {
    connectorProps = new Properties();
    connectorProps.setProperty(SolaceSourceConstants.SOL_HOST, String.format("tcp://%s:55555", NetworkPubSubPlusExtension.DOCKER_NET_PUBSUB_ALIAS));
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

  void messageToKafkaTest(Message msg, AbstractDestination destination, String expectedValue, Object expectedKey, KafkaContext kafkaContext) {
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
      LOG.info("Kafka message received - Key=" + record.key() + ", Value=" + record.value());
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
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.topics", "TestTopic1/SubTopic");
      connectorProps.setProperty("sol.username", "test");
      connectorProps.setProperty("sol.password", "test");
    }

    @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("1-Hello TextMessageToTopicTest world!");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "1-Hello TextMessageToTopicTest world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(new byte[] { '2', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'T',
          'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "2-Hello Topic world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { '3', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd',
          ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "3-Hello attached world!", null, kafkaContext);
    }

    @DisplayName("TextMessage-Queue-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("4-Hello TextmessageToKafkaTest world!");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "4-Hello TextmessageToKafkaTest world!", null, kafkaContext);
    }

    @DisplayName("BytesMessage-Queue-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerBytesmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(new byte[] { '5', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'Q',
          'u', 'e', 'u', 'e', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "5-Hello Queue world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Queue-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToQueueTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { '6', '-', 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd',
          ' ', 'w', 'o', 'r', 'l', 'd', '!' });
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
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
      connectorProps.setProperty("sol.kafka_message_key", "NONE");
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
          "Hello TextMessageToTopicTest1 world!", null, kafkaContext);
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
          "Hello Topic world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleKeyedMessageProcessor")
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

    @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextmessageToKafkaTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
      messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
          // expected value & key:
          "Hello TextmessageToKafkaTest world!", null, kafkaContext);
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
          "Hello Queue world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-AttachmentPayload-Queue-SolSampleKeyedMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageWithAttachmentPayloadToQueueTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(null);
      msg.writeAttachment(new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'a', 't', 't', 'a', 'c', 'h', 'e', 'd', ' ', 'w',
          'o', 'r', 'l', 'd', '!' });
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
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
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
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
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
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolaceSampleKeyedMessageProcessor");
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
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
      connectorProps.setProperty("sol.message_processor_class",
          "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.topics", "#share/group1/TestTopic1/SubTopic");
      connectorProps.setProperty("tasks.max", "5");
    }

    @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerTextMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest world!");
      messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
          // expected value & key:
          "Hello TextMessageToTopicTest world!", null, kafkaContext);
    }

    @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
    @ParameterizedTest
    @KafkaArgumentSource
    void kafkaConsumerByteMessageToTopicTest(KafkaContext kafkaContext) {
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps);
      BytesMessage msg = solaceProducer.createBytesMessage(
          new byte[] { 'H', 'e', 'l', 'l', 'o', ' ', 'T', 'o', 'p', 'i', 'c', ' ', 'w', 'o', 'r', 'l', 'd', '!' });
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
    private final Gson GSON = new GsonBuilder().setPrettyPrinting().create();

    @BeforeEach
    void setUp() {
      solaceProducer.resetQueue(SOL_QUEUE);
    }

    @ParameterizedTest
    @KafkaArgumentSource
    void testFailPubSubConnection(KafkaContext kafkaContext) {
      connectorProps.setProperty("sol.message_processor_class",
              "com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor");
      connectorProps.setProperty("sol.vpn_name", RandomStringUtils.randomAlphanumeric(10));
      kafkaContext.getSolaceConnectorDeployment().startConnector(connectorProps, true);
      AtomicReference<JsonObject> connectorStatus = new AtomicReference<>(new JsonObject());
      assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
        JsonObject taskStatus;
        do {
          JsonObject status = kafkaContext.getSolaceConnectorDeployment().getConnectorStatus();
          connectorStatus.set(status);
          taskStatus = status.getAsJsonArray("tasks").get(0).getAsJsonObject();
        } while (!taskStatus.get("state").getAsString().equals("FAILED"));
        assertThat(taskStatus.get("trace").getAsString(), containsString("Message VPN Not Allowed"));
      }, () -> "Timed out waiting for connector to fail: " + GSON.toJson(connectorStatus.get()));
    }
  }
}
