package com.solace.messaging.kafka.it;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;

public class DockerizedPlatformSetupApache implements MessagingServiceFullLocalSetupApache {

    @DisplayName("Local MessagingService connection tests")
    @Nested
    class MessagingServiceConnectionTests {

        @Container
        public final GenericContainer<?> connector = new FixedHostPortGenericContainer<>("bitnami/kafka:2")
                        .withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT",
                                        COMPOSE_CONTAINER_KAFKA.getServiceHost("zookeeper_1", 2181) + ":2181")
                        .withEnv("ALLOW_PLAINTEXT_LISTENER", "yes")
                        .withCommand("/bin/sh", "-c", //"sleep 10000")
                            "sed -i 's/bootstrap.servers=.*/bootstrap.servers=" + COMPOSE_CONTAINER_KAFKA.getServiceHost("kafka_1", 39092) 
                                + ":39092/g' /opt/bitnami/kafka/config/connect-distributed.properties; "
                                + "echo 'plugin.path=/opt/bitnami/kafka/jars' >> /opt/bitnami/kafka/config/connect-distributed.properties; "
                                + "echo 'rest.port=28083' >> /opt/bitnami/kafka/config/connect-distributed.properties; "
//                                + "sleep 10000")
                                + "/opt/bitnami/kafka/bin/connect-distributed.sh /opt/bitnami/kafka/config/connect-distributed.properties")
                        
                        .withFixedExposedPort(28083,28083)
//                        .withFixedExposedPort(5005,5005)
                        .withExposedPorts(28083)
//                        .withEnv("CONNECT_REST_PORT", "28083")
////
//                        // Enable remote debug session at default port 5005
//                        .withEnv("KAFKA_DEBUG", "y")
//                        .withEnv("DEBUG_SUSPEND_FLAG", "y")
////
//                        .withEnv("CONNECT_GROUP_ID", "quickstart-avro")
//                        .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "quickstart-avro-config")
//                        .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "quickstart-avro-offsets")
//                        .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "quickstart-avro-status")
//                        .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
//                        .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
//                        .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
//                        .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
//                        .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
//                        .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL",
//                                        "http://" + COMPOSE_CONTAINER_KAFKA.getServiceHost("schema-registry_1", 8081)
//                                                        + ":8081")
//                        .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
//                                        "http://" + COMPOSE_CONTAINER_KAFKA.getServiceHost("schema-registry_1", 8081)
//                                                        + ":8081")
//                        .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
//                        .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
////                        
//                        .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")
//                        .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
//                        .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/etc/kafka-connect/jars")
                        .withClasspathResourceMapping("pubsubplus-connector-kafka-source/lib",
                                        "/opt/bitnami/kafka/jars/pubsubplus-connector-kafka", BindMode.READ_ONLY)
////                        .waitingFor( Wait.forHealthcheck() );
                        .withStartupTimeout(Duration.ofSeconds(120))
                        .waitingFor( Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1) )
                          ;

        @DisplayName("Setup the dockerized platform")
        @Test
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
