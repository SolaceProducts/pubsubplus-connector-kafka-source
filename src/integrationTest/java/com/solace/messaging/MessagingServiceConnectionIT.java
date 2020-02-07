package com.solace.messaging;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;


public class MessagingServiceConnectionIT implements MessagingServiceFullLocalSetup {

  @DisplayName("Local MessagingService connection tests")
  @Nested
  class MessagingServiceConnectionTests {


      @DisplayName("Connect to a broker using local defaults")
      @Test
      void connectToRunningBrokerUsingLocalDefaultsIntegrationTest() {
          String host = COMPOSE_CONTAINER_PUBSUBPLUS.getServiceHost("solbroker_1", 8080);
          assertNotNull(host);

      }

      @DisplayName("Checking file availability")
      @Test
      void Test2() {
          File bundle = new File("../build/distributions/pubsubplus-connector-kafka-source.zip");
          assertNotNull(bundle);
      }
  }
}

