package com.solace.messaging.kafka.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

public class DockerizedPlatformSetupApache implements MessagingServiceFullLocalSetupApache {

  @Container
  public final static GenericContainer<?> KAFKA_CONNECT_REST = new FixedHostPortGenericContainer<>("bitnami/kafka:2")
                  .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", dockerIpAddress + ":2181")
                  .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
                  .withCommand("/bin/sh", "-c", //"sleep 10000")
                      "sed -i 's/bootstrap.servers=.*/bootstrap.servers=" + dockerIpAddress 
                          + ":39092/g' /opt/bitnami/kafka/config/connect-distributed.properties; "
                          + "echo 'plugin.path=/opt/bitnami/kafka/jars' >> /opt/bitnami/kafka/config/connect-distributed.properties; "
                          + "echo 'rest.port=28083' >> /opt/bitnami/kafka/config/connect-distributed.properties; "
                          + "/opt/bitnami/kafka/bin/connect-distributed.sh /opt/bitnami/kafka/config/connect-distributed.properties")
                  .withFixedExposedPort(28083,28083)
                  .withExposedPorts(28083)
////
//                  // Enable remote debug session at default port 5005
//                  .withEnv("KAFKA_DEBUG", "y")
//                  .withEnv("DEBUG_SUSPEND_FLAG", "y")
////
                  .withClasspathResourceMapping(Tools.getUnzippedConnectorDirName() + "/lib",
                                  "/opt/bitnami/kafka/jars/pubsubplus-connector-kafka", BindMode.READ_ONLY)
//                  .withStartupTimeout(Duration.ofSeconds(120))
                  .waitingFor( Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1) )
                    ;

  @BeforeAll
  static void setUp() {
    assert(KAFKA_CONNECT_REST != null);  // Required to instantiate 
  }

  @DisplayName("Local MessagingService connection tests")
  @Nested
  class MessagingServiceConnectionTests {
    @DisplayName("Setup the dockerized platform")
    @Test
    @Disabled
    void setupDockerizedPlatformTest() {
      String host = COMPOSE_CONTAINER_PUBSUBPLUS.getServiceHost("solbroker_1", 8080);
      assertNotNull(host);
      try {
        Thread.sleep(36000000l);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

    }
  }
}
