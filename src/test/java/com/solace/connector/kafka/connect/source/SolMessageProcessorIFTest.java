package com.solace.connector.kafka.connect.source;

import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_APPLICATION_MESSAGE_ID;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_APPLICATION_MESSAGE_TYPE;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_CORRELATION_ID;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_COS;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_DELIVERY_MODE;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_DESTINATION;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_REPLY_TO_DESTINATION;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_REPLY_TO_DESTINATION_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.User_Cos;
import com.solacesystems.jcsmp.impl.QueueImpl;
import com.solacesystems.jcsmp.impl.RawSMFMessageImpl;
import com.solacesystems.jcsmp.impl.TopicImpl;
import java.math.BigInteger;
import java.util.UUID;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SolMessageProcessorIFTest {

  private SolMessageProcessorIF messageProcessor;

  @BeforeEach
  void setUp() {
    messageProcessor = new SolSampleSimpleMessageProcessor();
  }

  @Test
  void testUserPropertiesMappingGivenNullUserPropertyMap() {
    final BytesXMLMessage message = mock(TextMessage.class);
    when(message.getProperties()).thenReturn(null);

    ConnectHeaders kafkaHeaders = messageProcessor.userPropertiesToKafkaHeaders(message);
    assertThat("getProperties() is null", kafkaHeaders.isEmpty());
  }

  @Test
  void testUserPropertiesMappingGiveEmptyUserPropertyMap() {
    final SDTMap solMsgUserProperties = JCSMPFactory.onlyInstance().createMap();
    final BytesXMLMessage message = mock(TextMessage.class);
    when(message.getProperties()).thenReturn(solMsgUserProperties);

    ConnectHeaders kafkaHeaders = messageProcessor.userPropertiesToKafkaHeaders(message);
    assertThat("solMsgUserProperties is empty", kafkaHeaders.isEmpty());
  }

  @Test
  void testUserPropertiesMappingForGivenUserPropertyMap() throws SDTException {
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
    solMsgUserProperties.putObject("bigInteger-user-property", new BigInteger("123456"));
    solMsgUserProperties.putByte("byte-user-property", "A".getBytes()[0]);
    solMsgUserProperties.putBytes("bytes-user-property", "Hello".getBytes());
    solMsgUserProperties.putByteArray("byteArray-user-property",
        new ByteArray("Hello World".getBytes()));
    solMsgUserProperties.putDestination("topic-user-property",
        TopicImpl.createFastNoValidation("testTopic"));
    solMsgUserProperties.putDestination("queue-user-property",
        QueueImpl.createFastNoValidation("testQueue"));

    final BytesXMLMessage message = mock(TextMessage.class);
    when(message.getProperties()).thenReturn(solMsgUserProperties);

    final ConnectHeaders kafkaHeaders = messageProcessor.userPropertiesToKafkaHeaders(message);
    assertThat(kafkaHeaders.size(), equalTo(message.getProperties().size()));

    kafkaHeaders.iterator().forEachRemaining(
        header -> assertThat(header.key(), solMsgUserProperties.containsKey(header.key())));
  }

  @Test
  void testUserPropertiesMappingWhenGivenPropertyOfUnsupportedTypes()
      throws SDTException {
    final SDTMap solMsgUserProperties = JCSMPFactory.onlyInstance().createMap();
    solMsgUserProperties.putMap("map-user-property", JCSMPFactory.onlyInstance().createMap());
    solMsgUserProperties.putStream("stream-user-property", JCSMPFactory.onlyInstance().createStream());
    solMsgUserProperties.putMessage("raw-message-user-property",
        new RawSMFMessageImpl(new ByteArray("hello".getBytes())));

    final BytesXMLMessage message = mock(TextMessage.class);
    when(message.getProperties()).thenReturn(solMsgUserProperties);

    ConnectHeaders kafkaHeaders = messageProcessor.userPropertiesToKafkaHeaders(message);
    assertThat(solMsgUserProperties.size(), equalTo(message.getProperties().size()));
    assertThat(kafkaHeaders.size(), equalTo(0));
  }

  @Test
  void testSolaceStandardPropertiesMappingGivenSolaceMessage() {
    final BytesXMLMessage message = mock(TextMessage.class);
    when(message.getApplicationMessageId()).thenReturn(UUID.randomUUID().toString());
    when(message.getApplicationMessageType()).thenReturn("testMessageType");
    when(message.getCorrelationId()).thenReturn(UUID.randomUUID().toString());
    when(message.getCos()).thenReturn(User_Cos.USER_COS_1);
    when(message.getDestination()).thenReturn(QueueImpl.createFastNoValidation("testQueue"));
    when(message.getDeliveryMode()).thenReturn(DeliveryMode.PERSISTENT);
    when(message.getReplyTo()).thenReturn(QueueImpl.createFastNoValidation("testQueue"));

    ConnectHeaders kafkaHeaders = messageProcessor.solacePropertiesToKafkaHeaders(message);
    assertThat("kafkaHeaders should not be empty", !kafkaHeaders.isEmpty());
    assertThat(message.getApplicationMessageId(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_APPLICATION_MESSAGE_ID).value()));
    assertThat(message.getApplicationMessageType(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_APPLICATION_MESSAGE_TYPE).value()));
    assertThat(message.getCorrelationId(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_CORRELATION_ID).value()));
    assertThat(message.getCos().value(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_COS).value()));
    assertThat(message.getDestination().getName(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_DESTINATION).value()));
    assertThat(message.getDeliveryMode().name(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_DELIVERY_MODE).value()));

    assertThat(message.getReplyTo().getName(),
        equalTo(kafkaHeaders.lastWithName(SOL_SH_REPLY_TO_DESTINATION).value()));
    assertThat("queue",
        equalTo(kafkaHeaders.lastWithName(SOL_SH_REPLY_TO_DESTINATION_TYPE).value()));
  }
}