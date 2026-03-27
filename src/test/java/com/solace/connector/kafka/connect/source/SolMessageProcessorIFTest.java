package com.solace.connector.kafka.connect.source;

import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_APPLICATION_MESSAGE_ID;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_APPLICATION_MESSAGE_TYPE;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_CORRELATION_ID;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_COS;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_DELIVERY_MODE;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_DESTINATION;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_REPLY_TO_DESTINATION;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_SH_REPLY_TO_DESTINATION_TYPE;
import static org.assertj.core.api.Assertions.assertThat;
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
import com.solacesystems.jcsmp.impl.RawSMFMessageImpl;
import java.math.BigInteger;
import java.util.UUID;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SolMessageProcessorIFTest {

  private SolMessageProcessorIF messageProcessor;

  @BeforeEach
  void setUp() {
    messageProcessor = new SolSampleSimpleMessageProcessor();
  }

  @Test
  void testUserPropertiesMappingGivenNullUserPropertyMap() {
    BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    assertThat(messageProcessor.userPropertiesToKafkaHeaders(message))
        .as("getProperties() is null")
        .isEmpty();
  }

  @Test
  void testUserPropertiesMappingGiveEmptyUserPropertyMap() {
    BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    message.setProperties(JCSMPFactory.onlyInstance().createMap());
    assertThat(messageProcessor.userPropertiesToKafkaHeaders(message))
        .as("solMsgUserProperties is empty")
        .isEmpty();
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
        JCSMPFactory.onlyInstance().createTopic("testTopic"));
    solMsgUserProperties.putDestination("queue-user-property",
        JCSMPFactory.onlyInstance().createQueue("testQueue"));

    BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    message.setProperties(solMsgUserProperties);

    assertThat(messageProcessor.userPropertiesToKafkaHeaders(message))
        .hasSize(message.getProperties().size())
        .allSatisfy(header -> assertThat(solMsgUserProperties.containsKey(header.key()))
            .isTrue());
  }

  @Test
  void testUserPropertiesMappingWhenGivenPropertyOfUnsupportedTypes()
      throws SDTException {
    final SDTMap solMsgUserProperties = JCSMPFactory.onlyInstance().createMap();
    solMsgUserProperties.putMap("map-user-property", JCSMPFactory.onlyInstance().createMap());
    solMsgUserProperties.putStream("stream-user-property", JCSMPFactory.onlyInstance().createStream());
    solMsgUserProperties.putMessage("raw-message-user-property",
        new RawSMFMessageImpl(new ByteArray("hello".getBytes())));

    BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    message.setProperties(solMsgUserProperties);

    ConnectHeaders kafkaHeaders = messageProcessor.userPropertiesToKafkaHeaders(message);
    assertThat(solMsgUserProperties.size()).isEqualTo(message.getProperties().size());
    assertThat(kafkaHeaders.size()).isZero();
  }

  @Test
  void testSolaceStandardPropertiesMappingGivenSolaceMessage(@Mock BytesXMLMessage message) {
    when(message.getApplicationMessageId()).thenReturn(UUID.randomUUID().toString());
    when(message.getApplicationMessageType()).thenReturn("testMessageType");
    when(message.getCorrelationId()).thenReturn(UUID.randomUUID().toString());
    when(message.getCos()).thenReturn(User_Cos.USER_COS_1);
    when(message.getDestination()).thenReturn(JCSMPFactory.onlyInstance().createQueue("testQueue"));
    when(message.getDeliveryMode()).thenReturn(DeliveryMode.PERSISTENT);
    when(message.getReplyTo()).thenReturn(JCSMPFactory.onlyInstance().createQueue("testQueue"));

    ConnectHeaders kafkaHeaders = messageProcessor.solacePropertiesToKafkaHeaders(message);
    assertThat(kafkaHeaders)
        .as("kafkaHeaders should not be empty")
        .isNotEmpty();
    assertThat(kafkaHeaders.lastWithName(SOL_SH_APPLICATION_MESSAGE_ID).value())
        .isEqualTo(message.getApplicationMessageId());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_APPLICATION_MESSAGE_TYPE).value())
        .isEqualTo(message.getApplicationMessageType());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_CORRELATION_ID).value())
        .isEqualTo(message.getCorrelationId());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_COS).value())
        .isEqualTo(message.getCos().value());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_DESTINATION).value())
        .isEqualTo(message.getDestination().getName());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_DELIVERY_MODE).value())
        .isEqualTo(message.getDeliveryMode().name());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_REPLY_TO_DESTINATION).value())
        .isEqualTo(message.getReplyTo().getName());
    assertThat(kafkaHeaders.lastWithName(SOL_SH_REPLY_TO_DESTINATION_TYPE).value())
        .isEqualTo("queue");
  }
}