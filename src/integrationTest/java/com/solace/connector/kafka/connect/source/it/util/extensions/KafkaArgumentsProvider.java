package com.solace.connector.kafka.connect.source.it.util.extensions;

import com.solace.connector.kafka.connect.source.it.SolaceConnectorDeployment;
import com.solace.connector.kafka.connect.source.it.util.KafkaConnection;
import com.solace.connector.kafka.connect.source.it.util.testcontainers.BitnamiKafkaConnectContainer;
import com.solace.connector.kafka.connect.source.it.util.testcontainers.ConfluentKafkaConnectContainer;
import com.solace.connector.kafka.connect.source.it.util.testcontainers.ConfluentKafkaControlCenterContainer;
import com.solace.connector.kafka.connect.source.it.util.testcontainers.ConfluentKafkaSchemaRegistryContainer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junitpioneer.jupiter.CartesianAnnotationConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class KafkaArgumentsProvider implements ArgumentsProvider,
		CartesianAnnotationConsumer<KafkaArgumentsProvider.KafkaArgumentSource> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaArgumentsProvider.class);

	@Override
	public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
		KafkaConnection bitnamiCxn = context.getRoot()
				.getStore(KafkaNamespace.BITNAMI.getNamespace())
				.getOrComputeIfAbsent(BitnamiResource.class, c -> {
					LOG.info("Creating Bitnami Kafka");
					BitnamiKafkaConnectContainer container = new BitnamiKafkaConnectContainer()
							.withNetwork(NetworkPubSubPlusExtension.DOCKER_NET);
					if (!container.isCreated()) {
						container.start();
					}
					return new BitnamiResource(container);
				}, BitnamiResource.class)
				.getKafkaConnection();

		KafkaConnection confluentCxn = context.getRoot()
				.getStore(KafkaNamespace.CONFLUENT.getNamespace())
				.getOrComputeIfAbsent(ConfluentResource.class, c -> {
					LOG.info("Creating Confluent Kafka");
					KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka")
							.withTag("6.2.1"))
							.withNetwork(NetworkPubSubPlusExtension.DOCKER_NET)
							.withNetworkAliases("kafka");
					if (!kafkaContainer.isCreated()) {
						kafkaContainer.start();
					}

					ConfluentKafkaSchemaRegistryContainer schemaRegistryContainer =
							new ConfluentKafkaSchemaRegistryContainer(kafkaContainer)
							.withNetworkAliases("schema-registry");
					if (!schemaRegistryContainer.isCreated()) {
						schemaRegistryContainer.start();
					}

					ConfluentKafkaControlCenterContainer controlCenterContainer =
							new ConfluentKafkaControlCenterContainer(kafkaContainer, schemaRegistryContainer);
					if (!controlCenterContainer.isCreated()) {
						controlCenterContainer.start();
					}

					ConfluentKafkaConnectContainer connectContainer =
							new ConfluentKafkaConnectContainer(kafkaContainer, schemaRegistryContainer);
					if (!connectContainer.isCreated()) {
						connectContainer.start();
					}
					return new ConfluentResource(
							new KafkaContainerResource<>(kafkaContainer),
							new KafkaContainerResource<>(schemaRegistryContainer),
							new KafkaContainerResource<>(controlCenterContainer),
							new KafkaContainerResource<>(connectContainer));
				}, ConfluentResource.class)
				.getKafkaConnection();

		return Stream.of(
			Arguments.of(createKafkaContext(bitnamiCxn, KafkaNamespace.BITNAMI, context)),
			Arguments.of(createKafkaContext(confluentCxn, KafkaNamespace.CONFLUENT, context))
		);
	}

	private KafkaContext createKafkaContext(KafkaConnection connection, KafkaNamespace namespace,
											ExtensionContext context) {
		AdminClient adminClient = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(AdminClientResource.class, c -> {
					LOG.info("Creating Kafka admin client for {}", connection.getBootstrapServers());
					Properties properties = new Properties();
					properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.getBootstrapServers());
					AdminClient newAdminClient = AdminClient.create(properties);
					return new AdminClientResource(newAdminClient);
				}, AdminClientResource.class)
				.getAdminClient();

		String kafkaTopic = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(TopicResource.class, c -> {
					String topicName = RandomStringUtils.randomAlphanumeric(100);
					LOG.info("Creating Kafka topic {}", topicName);
					try {
						adminClient.createTopics(Collections.singleton(new NewTopic(topicName, 5, (short) 1)))
								.all().get(5, TimeUnit.SECONDS);
					} catch (InterruptedException | ExecutionException | TimeoutException e) {
						throw new RuntimeException(e);
					}
					return new TopicResource(topicName, adminClient);
				}, TopicResource.class)
				.getTopicName();

		SolaceConnectorDeployment connectorDeployment = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(ConnectorDeploymentResource.class, c -> {
					SolaceConnectorDeployment deployment = new SolaceConnectorDeployment(connection, kafkaTopic);
					deployment.waitForConnectorRestIFUp();
					return new ConnectorDeploymentResource(deployment);
				}, ConnectorDeploymentResource.class)
				.getDeployment();

		KafkaConsumer<Object, Object> consumer = context.getRoot()
				.getStore(namespace.getNamespace())
				.getOrComputeIfAbsent(ConsumerResource.class, c -> {
					LOG.info("Creating Kafka consumer for {}", connection.getBootstrapServers());
					Properties properties = new Properties();
					properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection.getBootstrapServers());
					properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class
							.getName());
					properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
							.getName());
					properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, RandomStringUtils.randomAlphanumeric(50));
					properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

					// create consumer
					KafkaConsumer<Object, Object> newConsumer = new KafkaConsumer<>(properties);
					// subscribe consumer to our topic(s)
					newConsumer.subscribe(Collections.singleton(kafkaTopic));
					return new ConsumerResource(newConsumer);
				}, ConsumerResource.class)
				.getConsumer();

		return new KafkaContext(namespace, connection, adminClient, connectorDeployment, consumer);
	}

	@Override
	public void accept(KafkaArgumentSource kafkaArgumentSource) {

	}

	@Target(ElementType.METHOD)
	@Retention(RetentionPolicy.RUNTIME)
	@ArgumentsSource(KafkaArgumentsProvider.class)
	public @interface KafkaArgumentSource {

	}

	public static class AutoDeleteSolaceConnectorDeploymentAfterEach implements AfterEachCallback {
		@Override
		public void afterEach(ExtensionContext context) throws Exception {
			for (KafkaNamespace namespace : KafkaNamespace.values()) {
				ConnectorDeploymentResource deploymentResource = context.getRoot()
						.getStore(namespace.getNamespace())
						.get(ConnectorDeploymentResource.class, ConnectorDeploymentResource.class);
				if (deploymentResource != null) {
					deploymentResource.close();
				}
			}
		}
	}

	public static class KafkaContext {
		private final KafkaNamespace namespace;
		private final KafkaConnection connection;
		private final AdminClient adminClient;
		private final SolaceConnectorDeployment solaceConnectorDeployment;
		private final KafkaConsumer<Object, Object> consumer;

		private KafkaContext(KafkaNamespace namespace, KafkaConnection connection, AdminClient adminClient,
							 SolaceConnectorDeployment solaceConnectorDeployment,
							 KafkaConsumer<Object, Object> consumer) {
			this.namespace = namespace;
			this.connection = connection;
			this.consumer = consumer;
			this.solaceConnectorDeployment = solaceConnectorDeployment;
			this.adminClient = adminClient;
		}

		public KafkaConnection getConnection() {
			return connection;
		}

		public AdminClient getAdminClient() {
			return adminClient;
		}

		public SolaceConnectorDeployment getSolaceConnectorDeployment() {
			return solaceConnectorDeployment;
		}

		public KafkaConsumer<Object, Object> getConsumer() {
			return consumer;
		}

		@Override
		public String toString() {
			return namespace.name();
		}
	}

	private static class ConsumerResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(ConsumerResource.class);
		private final KafkaConsumer<Object, Object> consumer;

		private ConsumerResource(KafkaConsumer<Object, Object> consumer) {
			this.consumer = consumer;
		}


		public KafkaConsumer<Object, Object> getConsumer() {
			return consumer;
		}

		@Override
		public void close() {
			LOG.info("Closing Kafka consumer");
			consumer.close();
		}
	}

	private static class TopicResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(TopicResource.class);
		private final String topicName;
		private final AdminClient adminClient;

		private TopicResource(String topicName, AdminClient adminClient) {
			this.topicName = topicName;
			this.adminClient = adminClient;
		}

		public String getTopicName() {
			return topicName;
		}

		@Override
		public void close() throws Throwable {
			LOG.info("Deleting Kafka topic {}", topicName);
			DeleteTopicsResult result = adminClient.deleteTopics(Collections.singleton(topicName));
			for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
				try {
					entry.getValue().get(1, TimeUnit.MINUTES);
				} catch (ExecutionException e) {
					if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
						throw e;
					}
				}
			}
		}
	}

	private static class AdminClientResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(AdminClientResource.class);
		private final AdminClient adminClient;

		private AdminClientResource(AdminClient adminClient) {
			this.adminClient = adminClient;
		}

		public AdminClient getAdminClient() {
			return adminClient;
		}

		@Override
		public void close() {
			LOG.info("Closing Kafka admin client");
			adminClient.close();
		}
	}

	private static class ConnectorDeploymentResource implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(ConnectorDeploymentResource.class);
		private final SolaceConnectorDeployment deployment;

		private ConnectorDeploymentResource(SolaceConnectorDeployment deployment) {
			this.deployment = deployment;
		}

		public SolaceConnectorDeployment getDeployment() {
			return deployment;
		}

		@Override
		public void close() throws IOException {
			LOG.info("Closing Kafka connector deployment");
			deployment.deleteConnector();
		}
	}

	private static class BitnamiResource extends KafkaContainerResource<BitnamiKafkaConnectContainer> {

		private BitnamiResource(BitnamiKafkaConnectContainer container) {
			super(container);
		}

		public KafkaConnection getKafkaConnection() {
			return new KafkaConnection(getContainer().getBootstrapServers(), getContainer().getConnectUrl(),
					getContainer(), getContainer());
		}
	}

	private static class ConfluentResource implements CloseableResource {
		private final KafkaContainerResource<KafkaContainer> kafka;
		private final KafkaContainerResource<ConfluentKafkaSchemaRegistryContainer> schemaRegistry;
		private final KafkaContainerResource<ConfluentKafkaControlCenterContainer> controlCenter;
		private final KafkaContainerResource<ConfluentKafkaConnectContainer> connect;

		private ConfluentResource(KafkaContainerResource<KafkaContainer> kafka,
								  KafkaContainerResource<ConfluentKafkaSchemaRegistryContainer> schemaRegistry,
								  KafkaContainerResource<ConfluentKafkaControlCenterContainer> controlCenter,
								  KafkaContainerResource<ConfluentKafkaConnectContainer> connect) {
			this.kafka = kafka;
			this.schemaRegistry = schemaRegistry;
			this.controlCenter = controlCenter;
			this.connect = connect;
		}

		public KafkaConnection getKafkaConnection() {
			return new KafkaConnection(kafka.getContainer().getBootstrapServers(),
					connect.getContainer().getConnectUrl(), kafka.container, connect.container);
		}

		public KafkaContainerResource<KafkaContainer> getKafka() {
			return kafka;
		}

		public KafkaContainerResource<ConfluentKafkaConnectContainer> getConnect() {
			return connect;
		}

		@Override
		public void close() {
			connect.close();
			controlCenter.close();
			schemaRegistry.close();
			kafka.close();
		}
	}

	private static class KafkaContainerResource<T extends GenericContainer<?>> implements CloseableResource {
		private static final Logger LOG = LoggerFactory.getLogger(KafkaContainerResource.class);
		private final T container;

		private KafkaContainerResource(T container) {
			this.container = container;
		}

		public T getContainer() {
			return container;
		}

		@Override
		public void close() {
			LOG.info("Closing container {}", container.getContainerName());
			container.close();
		}
	}

	private enum KafkaNamespace {
		BITNAMI, CONFLUENT;

		private final Namespace namespace;

		KafkaNamespace() {
			this.namespace = Namespace.create(KafkaArgumentsProvider.class, name());
		}

		public Namespace getNamespace() {
			return namespace;
		}
	}
}
