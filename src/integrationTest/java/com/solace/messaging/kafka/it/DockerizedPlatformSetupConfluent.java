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

public class DockerizedPlatformSetupConfluent implements MessagingServiceFullLocalSetupConfluent {

  @Container
  public final static GenericContainer<?> connector = new FixedHostPortGenericContainer<>(
      "confluentinc/cp-kafka-connect-base:5.4.0")
          .withEnv("CONNECT_BOOTSTRAP_SERVERS", COMPOSE_CONTAINER_KAFKA.getServiceHost("kafka_1", 39092) + ":39092")
          .withFixedExposedPort(28083, 28083).withFixedExposedPort(5005, 5005).withExposedPorts(28083, 5005)
          .withEnv("CONNECT_REST_PORT", "28083")
//
//                        // Enable remote debug session at default port 5005
//                        .withEnv("KAFKA_DEBUG", "y")
//                        .withEnv("DEBUG_SUSPEND_FLAG", "y")
//
          .withEnv("CONNECT_GROUP_ID", "testconnect-avro")
          .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "testconnect-avro-config")
          .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "testconnect-avro-offsets")
          .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "testconnect-avro-status")
          .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
          .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
          .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
//                       .withEnv("CONNECT_OFFSET_FLUSH_INTERVAL_MS", "100")
          .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
          .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
          .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL",
              "http://" + COMPOSE_CONTAINER_KAFKA.getServiceHost("schema-registry_1", 8081) + ":8081")
          .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
              "http://" + COMPOSE_CONTAINER_KAFKA.getServiceHost("schema-registry_1", 8081) + ":8081")
          .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
          .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
//                        
          .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost").withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
          .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/etc/kafka-connect/jars")
          .withClasspathResourceMapping(Tools.getUnzippedConnectorDirName() + "/lib",
              "/etc/kafka-connect/jars/pubsubplus-connector-kafka", BindMode.READ_ONLY)
//                        .waitingFor( Wait.forHealthcheck() );
          .waitingFor(Wait.forLogMessage(".*Kafka Connect started.*", 1));

  @BeforeAll
  static void setUp() {
    assert(connector != null);
  }

  @DisplayName("Local MessagingService connection tests")
  @Nested
  class MessagingServiceConnectionTests {
    @DisplayName("Setup the dockerized platform")
    @Test
//    @Disabled
    void setupDockerizedPlatformTest() {
      String host = COMPOSE_CONTAINER_PUBSUBPLUS.getServiceHost("solbroker_1", 8080);
      assertNotNull(host);
      try {
        Thread.sleep(36000000l);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

    }
  }
}
