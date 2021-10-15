package com.solace.connector.kafka.connect.source.it.util;

import org.testcontainers.containers.GenericContainer;

import java.util.Objects;

public class KafkaConnection {
	private final String bootstrapServers;
	private final String connectUrl;
	private final GenericContainer<?> kafkaContainer;
	private final GenericContainer<?> connectContainer;

	public KafkaConnection(String bootstrapServers, String connectUrl, GenericContainer<?> kafkaContainer,
						   GenericContainer<?> connectContainer) {
		this.bootstrapServers = bootstrapServers;
		this.connectUrl = connectUrl;
		this.kafkaContainer = kafkaContainer;
		this.connectContainer = connectContainer;
	}

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public String getConnectUrl() {
		return connectUrl;
	}

	public GenericContainer<?> getKafkaContainer() {
		return kafkaContainer;
	}

	public GenericContainer<?> getConnectContainer() {
		return connectContainer;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		KafkaConnection that = (KafkaConnection) o;
		return Objects.equals(bootstrapServers, that.bootstrapServers) &&
				Objects.equals(connectUrl, that.connectUrl) &&
				Objects.equals(kafkaContainer, that.kafkaContainer) &&
				Objects.equals(connectContainer, that.connectContainer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(bootstrapServers, connectUrl, kafkaContainer, connectContainer);
	}
}
