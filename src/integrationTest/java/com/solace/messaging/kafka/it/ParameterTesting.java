package com.solace.messaging.kafka.it;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.solace.source.connector.SolaceSourceTask;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

public class ParameterTesting implements TestConstants {

    
    @DisplayName("Default Parameter test")
    @Test
    void CheckDefaultParams() {
        SolaceSourceTask testSourceTask = new SolaceSourceTask();
        
        Map<String, String> props = new HashMap<String, String>();
        /* ("sol.host", "tcp://" + MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_PUBSUBPLUS
                            .getServiceHost("solbroker_1", 55555) + ":55555");
            jobject.addProperty("sol.username", SOL_ADMINUSER_NAME);
            jobject.addProperty("sol.password", SOL_ADMINUSER_PW);
            jobject.addProperty("sol.vpn_name", SOL_VPN); */
        props.put("sol.host", "tcp://" + MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_PUBSUBPLUS
                        .getServiceHost("solbroker_1", 55555) + ":55555");
        props.put("sol.username", SOL_ADMINUSER_NAME);
        props.put("sol.password", SOL_ADMINUSER_PW);
        props.put("sol.vpn_name", SOL_VPN);
        
        testSourceTask.start(props);
        JCSMPSession solSession = testSourceTask.getSolSession();
        assert(!solSession.isClosed());
        JCSMPChannelProperties chanProperties = 
                        (JCSMPChannelProperties) solSession.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
        boolean GENERATE_SEND_TIMESTAMPS = (boolean) solSession.getProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS);
        solSession.getProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS);
        solSession.getProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS);
        solSession.getProperty(JCSMPProperties.CALCULATE_MESSAGE_EXPIRATION);
        solSession.getProperty(JCSMPProperties.PUB_MULTI_THREAD);
        solSession.getProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR);
        solSession.getProperty(JCSMPProperties.IGNORE_DUPLICATE_SUBSCRIPTION_ERROR);
        solSession.getProperty(JCSMPProperties.IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR);
        solSession.getProperty(JCSMPProperties.NO_LOCAL);
        solSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);
        solSession.getProperty(JCSMPProperties.SUBSCRIBER_LOCAL_PRIORITY);
        solSession.getProperty(JCSMPProperties.SUBSCRIBER_NETWORK_PRIORITY);
        solSession.getProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS);
        solSession.getProperty(JCSMPProperties.AUTHENTICATION_SCHEME);
        solSession.getProperty(JCSMPProperties.KRB_SERVICE_NAME);
        solSession.getProperty(JCSMPProperties.SSL_CONNECTION_DOWNGRADE_TO);
        solSession.getProperty(JCSMPProperties.SSL_CIPHER_SUITES);
        solSession.getProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE);
        solSession.getProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE);
        solSession.getProperty(JCSMPProperties.SSL_TRUST_STORE);
        solSession.getProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD);
        solSession.getProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT);
        solSession.getProperty(JCSMPProperties.SSL_TRUSTED_COMMON_NAME_LIST);
        solSession.getProperty(JCSMPProperties.SSL_KEY_STORE);
        solSession.getProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD);
        solSession.getProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT);
        solSession.getProperty(JCSMPProperties.SSL_KEY_STORE_NORMALIZED_FORMAT);
        solSession.getProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD);

        
        
        
        
        testSourceTask.stop();
    }

}
