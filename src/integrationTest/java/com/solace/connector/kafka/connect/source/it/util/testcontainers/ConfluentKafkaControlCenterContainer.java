package com.solace.connector.kafka.connect.source.it.util.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfluentKafkaControlCenterContainer extends GenericContainer<ConfluentKafkaControlCenterContainer> {
	private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-enterprise-control-center");
	private static final String DEFAULT_IMAGE_TAG = "6.2.1";

	public ConfluentKafkaControlCenterContainer(KafkaContainer kafka,
												ConfluentKafkaSchemaRegistryContainer schemaRegistry) {
		this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_IMAGE_TAG), kafka, schemaRegistry);
	}

	public ConfluentKafkaControlCenterContainer(DockerImageName dockerImageName,
												KafkaContainer kafka,
												ConfluentKafkaSchemaRegistryContainer schemaRegistry) {
		super(dockerImageName);
		assertThat(kafka.getNetworkAliases().size(), greaterThanOrEqualTo(2));
		assertThat(schemaRegistry.getNetworkAliases().size(), greaterThanOrEqualTo(2));
		assertEquals(kafka.getNetwork(), schemaRegistry.getNetwork());

		dependsOn(kafka, schemaRegistry);
		withNetwork(kafka.getNetwork());
		withEnv("CONTROL_CENTER_REPLICATION_FACTOR", "1");
		withEnv("CONTROL_CENTER_INTERNAL_TOPICS_PARTITIONS", "1");
		withEnv("CONTROL_CENTER_MONITORING_INTERCEPTOR_TOPIC_PARTITIONS", "1");
		withEnv("CONFLUENT_METRICS_TOPIC_REPLICATION", "1");
		withEnv("CONTROL_CENTER_SCHEMA_REGISTRY_URL", String.format("http://%s:%s",
				schemaRegistry.getNetworkAliases().get(1), ConfluentKafkaSchemaRegistryContainer.REGISTRY_PORT));
		withEnv("CONTROL_CENTER_BOOTSTRAP_SERVERS", String.format("%s:9092", kafka.getNetworkAliases().get(1)));
		withEnv("CONTROL_CENTER_ZOOKEEPER_CONNECT", String.format("%s:%s", kafka.getNetworkAliases().get(1),
				KafkaContainer.ZOOKEEPER_PORT));
	}
}
