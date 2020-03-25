package com.solace.messaging.kafka.it;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class TestConfigProperties {

    static String testConfigPropertiesFile = "src/integrationTest/resources/manual-setup.properties";
    
    private Properties properties = new Properties();

    TestConfigProperties() {
        try(FileReader fileReader = new FileReader(testConfigPropertiesFile)){
            properties.load(fileReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    String getProperty(String name) {
        String configuredProperty = properties.getProperty(name);
        if (configuredProperty != null) {
            return configuredProperty;
        }
        switch(name) {
            case "sol.host":
                // No port here
                return MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_PUBSUBPLUS
                                .getServiceHost("solbroker_1", 55555);
                
            case "sol.username":
                return "default";
                
            case "sol.password":
                return "default";
                
            case "sol.vpn_name":
                return "default";
                
            case "kafka.connect_rest_url":
                return MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_KAFKA
                                .getServiceHost("kafka_1", 9092) + ":28083";

            case "kafka.bootstrap_servers":
                return MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_KAFKA
                                .getServiceHost("kafka_1", 39092) + ":39092";

            default:
                return null;
        }
    }
}
