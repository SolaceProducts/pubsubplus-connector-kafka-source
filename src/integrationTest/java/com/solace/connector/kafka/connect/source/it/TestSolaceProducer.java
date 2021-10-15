package com.solace.connector.kafka.connect.source.it;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
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
    private XMLMessageProducer producer;

    public TestSolaceProducer(JCSMPSession session) {
        this.session = session;
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
        producer.send(msg,topic);
        logger.info("Message sent to Solace topic " + topic.toString());
    }

    public void resetQueue(String queueName) {
        try {
          final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
          // First remove existing queue potentially containing remainings from other tests
          session.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
          // Provision new queue
          final EndpointProperties endpointProps = new EndpointProperties();
          endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
          endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
          session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
          logger.info("Reset Solace queue " + queueName);
        } catch (JCSMPException e) {
          e.printStackTrace();
        }
    }

    public void sendMessageToQueue(Queue queue, Message msg) throws JCSMPException {
        msg.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(msg,queue);
        logger.info("Message sent to Solace queue " + queue.toString());
    }

    @Override
    public void close() {
        producer.close();
    }
}
