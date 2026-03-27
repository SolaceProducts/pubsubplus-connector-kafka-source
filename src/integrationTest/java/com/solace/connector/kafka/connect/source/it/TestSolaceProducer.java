package com.solace.connector.kafka.connect.source.it;

import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue.AccessTypeEnum;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue.PermissionEnum;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSolaceProducer implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(TestSolaceProducer.class);
  private final JCSMPSession session;
  private final SempV2Api sempV2Api;
  private XMLMessageProducer producer;

  public TestSolaceProducer(JCSMPSession session, SempV2Api sempV2Api) {
    this.session = session;
    this.sempV2Api = sempV2Api;
  }


  public void start() throws JCSMPException {
    producer = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
      @Override
      public void responseReceivedEx(Object correlationKey) {
        logger.info("Producer received response for msg: " + correlationKey);
      }

      @Override
      public void handleErrorEx(Object correlationKey, JCSMPException e, long timestamp) {
        logger.error("Producer received error for msg: {} {}", correlationKey, timestamp, e);
      }
    });
  }

  public TextMessage createTextMessage(String contents) {
    TextMessage textMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    textMessage.setText(contents);
    return textMessage;
  }

  public BytesMessage createBytesMessage(byte[] contents) {
    BytesMessage bytesMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
    bytesMessage.setData(contents);
    return bytesMessage;
  }

  public Topic defineTopic(String topicName) {
    return JCSMPFactory.onlyInstance().createTopic(topicName);
  }

  public Queue defineQueue(String queueName) {
    return JCSMPFactory.onlyInstance().createQueue(queueName);
  }

  public void sendMessageToTopic(Topic topic, Message msg) throws JCSMPException {
    producer.send(msg, topic);
    logger.info("Message sent to Solace topic " + topic.toString());
  }

  public void resetQueue(String queueName) throws ApiException {
    String vpnName = (String) session.getProperty(JCSMPProperties.VPN_NAME);

    // First remove existing queue potentially remaining from other tests
    try {
      sempV2Api.config().deleteMsgVpnQueue(vpnName, queueName);
    } catch (ApiException e) {
      if (!e.getResponseBody().contains("NOT_FOUND")) {
        throw e;
      }
      logger.debug("Queue {} not found", queueName);
    }

    ConfigMsgVpnQueue config = new ConfigMsgVpnQueue()
        .queueName(queueName)
        .permission(PermissionEnum.CONSUME)
        .accessType(AccessTypeEnum.NON_EXCLUSIVE)
        .redeliveryEnabled(false)
        .egressEnabled(true)
        .ingressEnabled(true);

    sempV2Api.config().createMsgVpnQueue(vpnName, config, null, null);
    logger.info("Reset Solace queue {}", queueName);
  }

  public void sendMessageToQueue(Queue queue, Message msg) throws JCSMPException {
    msg.setDeliveryMode(DeliveryMode.PERSISTENT);
    producer.send(msg, queue);
    logger.info("Message sent to Solace queue " + queue.toString());
  }

  @Override
  public void close() {
    producer.close();
  }
}
