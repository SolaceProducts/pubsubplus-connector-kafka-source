package com.solace.connector.kafka.connect.source.it;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class TestSolaceProducer {
    
    static Logger logger = LoggerFactory.getLogger(SourceConnectorIT.class.getName());
    private JCSMPSession session;
    private XMLMessageProducer producer;

    public void setup() {
        TestConfigProperties configProps = new TestConfigProperties();
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, "tcp://" + configProps.getProperty("sol.host") + ":55555");     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, configProps.getProperty("sol.username")); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  configProps.getProperty("sol.vpn_name")); // message-vpn
        properties.setProperty(JCSMPProperties.PASSWORD, configProps.getProperty("sol.password")); // client-password
        try {
            session =  JCSMPFactory.onlyInstance().createSession(properties);
            session.connect();
            producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
                @Override
                public void responseReceived(String messageID) {
                    logger.info("Producer received response for msg: " + messageID);
                }
                @Override
                public void handleError(String messageID, JCSMPException e, long timestamp) {
                    logger.info("Producer received error for msg: %s@%s - %s%n",
                            messageID,timestamp,e);
                }
            });
        } catch (JCSMPException e1) {
            e1.printStackTrace();
        }
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
    
    public void close() {
        session.closeSession();
    }
}
