package com.solace.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    static Logger logger = LoggerFactory.getLogger(ConnectorIT.class.getName());
    private JCSMPSession session;
    private XMLMessageProducer producer;

    public TestSolaceProducer(String host, String user, String password, String messagevpn) {
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, host);     // host:port
        properties.setProperty(JCSMPProperties.USERNAME, user); // client-username
        properties.setProperty(JCSMPProperties.VPN_NAME,  messagevpn); // message-vpn
        properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
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
    
    public void sendMessageToTopic(String topicName, Message msg) throws JCSMPException {
        final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
        producer.send(msg,topic);
        logger.info("Message sent to Solace topic " + topic);
    }
    
    public void provisionQueue(String queueName) throws JCSMPException {
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        // Provision queue in case it doesn't exist, and do not fail if it already exists
        final EndpointProperties endpointProps = new EndpointProperties();
        endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
        endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
        session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
        logger.info("Ensured Solace queue " + queueName + "exists.");
    }
    
    public void sendMessageToQueue(String queueName, Message msg) throws JCSMPException {
        final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
        msg.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(msg,queue);
        logger.info("Message sent to Solace queue " + queueName);
    }
    
    public void close() {
        session.closeSession();
    }
}
