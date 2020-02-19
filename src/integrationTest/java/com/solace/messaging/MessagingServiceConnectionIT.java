package com.solace.messaging;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

public class MessagingServiceConnectionIT implements MessagingServiceFullLocalSetup {

    @DisplayName("Local MessagingService connection tests")
    @Nested
    class MessagingServiceConnectionTests {

        @Container
        public final GenericContainer connector = new GenericContainer("confluentinc/cp-kafka-connect-base:5.4.0")
                        .withEnv("CONNECT_BOOTSTRAP_SERVERS",
                                        COMPOSE_CONTAINER_KAFKA.getServiceHost("kafka_1", 9092) + ":9092")
                        .withEnv("CONNECT_REST_PORT", "28083").withEnv("CONNECT_GROUP_ID", "quickstart-avro")
                        .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "quickstart-avro-config")
                        .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "quickstart-avro-offsets")
                        .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "quickstart-avro-status")
                        .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
                        .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
                        .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
                        .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                        .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
                        .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL",
                                        "http://" + COMPOSE_CONTAINER_KAFKA.getServiceHost("schema-registry_1", 8081)
                                                        + ":8081")
                        .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL",
                                        "http://" + COMPOSE_CONTAINER_KAFKA.getServiceHost("schema-registry_1", 8081)
                                                        + ":8081")
                        .withEnv("CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                        .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                        .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost")
                        .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
                        .withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/etc/kafka-connect/jars")
                        .withClasspathResourceMapping("pubsubplus-connector-kafka-source/lib",
                                        "/etc/kafka-connect/jars/pubsubplus-connector-kafka", BindMode.READ_ONLY)
                        .withExtraHost("kafka", COMPOSE_CONTAINER_KAFKA.getServiceHost("kafka_1", 9092))
                        .withExposedPorts(28083);

        @DisplayName("Connect to a broker using local defaults")
        @Test
        void connectToRunningBrokerUsingLocalDefaultsIntegrationTest() {
            String host = COMPOSE_CONTAINER_PUBSUBPLUS.getServiceHost("solbroker_1", 8080);
            assertNotNull(host);

        }

        @DisplayName("Running Kafka consumer")
        @Test
        void runTestKafkaConsumer() {
            TestKafkaConsumer kc = new TestKafkaConsumer();
            // Start consumer
            kc.run();
            try {
                Thread.sleep(1000000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            kc.stop();
        }
    }
}
