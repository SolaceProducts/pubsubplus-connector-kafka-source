package com.solace.connector.kafka.connect.source.it.util.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.solace.connector.kafka.connect.source.SolaceSourceTask;
import com.solace.connector.kafka.connect.source.it.Tools;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class BitnamiKafkaConnectContainer extends GenericContainer<BitnamiKafkaConnectContainer> {
	private static final String BROKER_LISTENER_NAME = "PLAINTEXT";
	private static final int BROKER_LISTENER_PORT = 9092;
	private static final String BOOTSTRAP_LISTENER_NAME = "PLAINTEXT_HOST";
	public static final int BOOTSTRAP_LISTENER_PORT = 29092;
	public static final int CONNECT_PORT = 28083;
	private static final int ZOOKEEPER_PORT = 2181;
	private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("bitnami/kafka");
	private static final String DEFAULT_IMAGE_TAG = "2";
	private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
	private DockerImageName zookeeperDockerImageName = DockerImageName.parse("bitnami/zookeeper:3");
	private GenericContainer<?> zookeeperContainer;

	public BitnamiKafkaConnectContainer() {
		this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_IMAGE_TAG));
	}

	public BitnamiKafkaConnectContainer(String dockerImageName) {
		this(DockerImageName.parse(dockerImageName));
	}

	public BitnamiKafkaConnectContainer(DockerImageName dockerImageName) {
		super(dockerImageName);

		withNetwork(Network.newNetwork());
		withExposedPorts(CONNECT_PORT, BROKER_LISTENER_PORT, BOOTSTRAP_LISTENER_PORT);
		withEnv("KAFKA_CFG_BROKER_ID", "1");
		withEnv("ALLOW_PLAINTEXT_LISTENER", "yes");
		withEnv("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", String.join(",",
				BROKER_LISTENER_NAME + ":PLAINTEXT", BOOTSTRAP_LISTENER_NAME + ":PLAINTEXT"));
		withEnv("KAFKA_CFG_LISTENERS", String.join(",",
				BROKER_LISTENER_NAME + "://:" + BROKER_LISTENER_PORT, BOOTSTRAP_LISTENER_NAME + "://:" + BOOTSTRAP_LISTENER_PORT));
		withClasspathResourceMapping(Tools.getUnzippedConnectorDirName() + "/lib",
				"/opt/bitnami/kafka/jars/pubsubplus-connector-kafka", BindMode.READ_ONLY);
		waitingFor(Wait.forLogMessage(".*Finished starting connectors and tasks.*", 1));
	}

	@Override
	public void start() {
		if (zookeeperDockerImageName != null) {
			String zookeeperNetworkAlias = "zookeeper";
			zookeeperContainer = new GenericContainer<>(zookeeperDockerImageName)
					.withNetwork(getNetwork())
					.withNetworkAliases(zookeeperNetworkAlias)
					.withEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_PORT))
					.withEnv("ZOOKEEPER_TICK_TIME", "2000")
					.withEnv("ALLOW_ANONYMOUS_LOGIN", "yes");
			dependsOn(zookeeperContainer);
			withEnv("KAFKA_CFG_ZOOKEEPER_CONNECT", String.format("%s:%s", zookeeperNetworkAlias, ZOOKEEPER_PORT));
		}
		super.start();
	}

	@Override
	protected void doStart() {
		// Delay starting Kafka until after container has started
		withCommand("sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
		super.doStart();
	}

	@Override
	protected void containerIsStarting(InspectContainerResponse containerInfo) {
		String command = "/bin/sh\n" +
				"set -e\n" +
				"echo 'plugin.path=/opt/bitnami/kafka/jars' >> /opt/bitnami/kafka/config/connect-distributed.properties\n" +
				"echo 'rest.port=" + CONNECT_PORT + "' >> /opt/bitnami/kafka/config/connect-distributed.properties\n" +
				"echo 'log4j.logger.org.apache.kafka.connect.runtime.WorkerSinkTask=DEBUG' >> /opt/bitnami/kafka/config/connect-log4j.properties\n" +
				"echo 'log4j.logger." + SolaceSourceTask.class.getName() + "=TRACE' >> /opt/bitnami/kafka/config/connect-log4j.properties\n" +
				"export KAFKA_CFG_ADVERTISED_LISTENERS=" + advertisedListeners(containerInfo) + "\n" +
				"/opt/bitnami/scripts/kafka/setup.sh\n" +
				"/opt/bitnami/scripts/kafka/run.sh &\n" +
				"/opt/bitnami/kafka/bin/connect-distributed.sh /opt/bitnami/kafka/config/connect-distributed.properties\n";
		copyFileToContainer(Transferable.of(command.getBytes(StandardCharsets.UTF_8), 0777), STARTER_SCRIPT);
		super.containerIsStarting(containerInfo);
	}

	@Override
	public void close() {
		super.close();
		if (zookeeperContainer != null) {
			zookeeperContainer.close();
		}
	}

	public String getBootstrapServers() {
		return String.format("%s:%s", getHost(), getMappedPort(BitnamiKafkaConnectContainer.BOOTSTRAP_LISTENER_PORT));
	}

	public String getConnectUrl() {
		return String.format("http://%s:%s", getHost(), getMappedPort(BitnamiKafkaConnectContainer.CONNECT_PORT));
	}

	public BitnamiKafkaConnectContainer withZookeeper(DockerImageName dockerImageName) {
		zookeeperDockerImageName = dockerImageName;
		return this;
	}

	private String advertisedListeners(InspectContainerResponse containerInfo) {
		return String.join(",",
				String.format("%s://%s:%s", BROKER_LISTENER_NAME, getExternalIpAddress(containerInfo), BROKER_LISTENER_PORT),
				String.format("%s://%s:%s", BOOTSTRAP_LISTENER_NAME, getHost(), getMappedPort(BOOTSTRAP_LISTENER_PORT)));
	}

	/**
	 * @see org.testcontainers.containers.KafkaContainer
	 */
	private String getExternalIpAddress(InspectContainerResponse containerInfo) {
		// Kafka supports only one INTER_BROKER listener, so we have to pick one.
		// The current algorithm uses the following order of resolving the IP:
		// 1. Custom network's IP set via `withNetwork`
		// 2. Bridge network's IP
		// 3. Best effort fallback to getNetworkSettings#ipAddress
		return containerInfo.getNetworkSettings().getNetworks().entrySet()
				.stream()
				.filter(it -> it.getValue().getIpAddress() != null)
				.max(Comparator.comparingInt(entry -> {
					if (getNetwork().getId().equals(entry.getValue().getNetworkID())) {
						return 2;
					}

					if ("bridge".equals(entry.getKey())) {
						return 1;
					}

					return 0;
				}))
				.map(it -> it.getValue().getIpAddress())
				.orElseGet(() -> containerInfo.getNetworkSettings().getIpAddress());
	}
}
