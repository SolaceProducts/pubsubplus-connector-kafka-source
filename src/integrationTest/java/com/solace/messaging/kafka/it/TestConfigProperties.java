package com.solace.messaging.kafka.it;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class TestConfigProperties {

    static String testConfigPropertiesFile = "src/integrationTest/resources/manual-setup.properties";
    // This class helps determine the docker host's IP address and avoids getting "localhost"
    static class DockerHost {
      static public String getIpAddress() {
        String dockerReportedAddress = MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_KAFKA
            .getServiceHost("kafka_1", 9092);
        if (dockerReportedAddress == "localhost" || dockerReportedAddress == "127.0.0.1") {
          try {
            return InetAddress.getLocalHost().getHostAddress();
          } catch (UnknownHostException e) {
             e.printStackTrace();
             return null;
          }
        } else {
          return MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_KAFKA
              .getServiceHost("kafka_1", 9092);
        }
      }   
    }
    
    
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
                return DockerHost.getIpAddress();
                
            case "sol.username":
                return "default";
                
            case "sol.password":
                return "default";
                
            case "sol.vpn_name":
                return "default";
                
            case "kafka.connect_rest_url":
                return (DockerHost.getIpAddress() + ":28083");

            case "kafka.bootstrap_servers":
                return (DockerHost.getIpAddress() + ":39092");

            default:
                return null;
        }
    }
}
