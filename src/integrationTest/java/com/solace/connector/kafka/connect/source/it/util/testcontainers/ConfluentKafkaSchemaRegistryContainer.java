package com.solace.connector.kafka.connect.source.it.util.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConfluentKafkaSchemaRegistryContainer extends GenericContainer<ConfluentKafkaSchemaRegistryContainer> {
	public static final int REGISTRY_PORT = 8081;
	private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("confluentinc/cp-schema-registry");
	private static final String DEFAULT_IMAGE_TAG = "7.4.1";

	public ConfluentKafkaSchemaRegistryContainer(KafkaContainer kafka) {
		this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_IMAGE_TAG), kafka);
	}

	public ConfluentKafkaSchemaRegistryContainer(DockerImageName dockerImageName, KafkaContainer kafka) {
		super(dockerImageName);

		assertNotNull(kafka.getNetwork());
		assertThat(kafka.getNetworkAliases().size(), greaterThanOrEqualTo(2));

		dependsOn(kafka);
		withNetwork(kafka.getNetwork());
		withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + REGISTRY_PORT);
		withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", String.format("PLAINTEXT://%s:9092",
				kafka.getNetworkAliases().get(1)));
		waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
	}

	@Override
	protected void doStart() {
		withEnv("SCHEMA_REGISTRY_HOST_NAME", getNetworkAliases().size() > 1 ? getNetworkAliases().get(1) : getHost());
		super.doStart();
	}
}
