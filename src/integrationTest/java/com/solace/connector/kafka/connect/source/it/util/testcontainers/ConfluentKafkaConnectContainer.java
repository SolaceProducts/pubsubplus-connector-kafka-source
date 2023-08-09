package com.solace.connector.kafka.connect.source.it.util.testcontainers;

import com.solace.connector.kafka.connect.source.it.Tools;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfluentKafkaConnectContainer extends GenericContainer<ConfluentKafkaConnectContainer> {
	public static final int CONNECT_PORT = 8083;
	private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-kafka-connect-base");
	private static final String DEFAULT_IMAGE_TAG = "7.4.1";

	public ConfluentKafkaConnectContainer(KafkaContainer kafka,
										  ConfluentKafkaSchemaRegistryContainer schemaRegistry) {
		this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_IMAGE_TAG), kafka, schemaRegistry);
	}

	public ConfluentKafkaConnectContainer(DockerImageName dockerImageName,
										  KafkaContainer kafka,
										  ConfluentKafkaSchemaRegistryContainer schemaRegistry) {
		super(dockerImageName);
		assertThat(kafka.getNetworkAliases().size(), greaterThanOrEqualTo(2));
		assertThat(schemaRegistry.getNetworkAliases().size(), greaterThanOrEqualTo(2));
		assertEquals(kafka.getNetwork(), schemaRegistry.getNetwork());

		dependsOn(kafka, schemaRegistry);
		withNetwork(kafka.getNetwork());
		withExposedPorts(CONNECT_PORT);
		withEnv("CONNECT_REST_PORT", Integer.toString(CONNECT_PORT));
		withEnv("CONNECT_GROUP_ID", "quickstart-avro");
		withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "quickstart-avro-config");
		withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "quickstart-avro-offsets");
		withEnv("CONNECT_STATUS_STORAGE_TOPIC", "quickstart-avro-status");
		withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
		withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
		withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
		withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter");
		withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter");
		withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", String.format("http://%s:%s",
				schemaRegistry.getNetworkAliases().get(1), ConfluentKafkaSchemaRegistryContainer.REGISTRY_PORT));
		withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", String.format("http://%s:%s",
				schemaRegistry.getNetworkAliases().get(1), ConfluentKafkaSchemaRegistryContainer.REGISTRY_PORT));
		withEnv("CONNECT_BOOTSTRAP_SERVERS", String.format("%s:9092", kafka.getNetworkAliases().get(1)));
		withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost");
		withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO");
		withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/etc/kafka-connect/jars");
		withClasspathResourceMapping(Tools.getUnzippedConnectorDirName() + "/lib",
				"/etc/kafka-connect/jars", BindMode.READ_ONLY);
		waitingFor( Wait.forLogMessage(".*Kafka Connect started.*", 1) );
	}

	public String getConnectUrl() {
		return String.format("http://%s:%s", getHost(), getMappedPort(ConfluentKafkaConnectContainer.CONNECT_PORT));
	}
}
