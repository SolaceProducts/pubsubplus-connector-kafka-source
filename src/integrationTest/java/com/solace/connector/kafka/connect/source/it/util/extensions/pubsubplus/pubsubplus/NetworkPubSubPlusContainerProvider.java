package com.solace.connector.kafka.connect.source.it.util.extensions.pubsubplus.pubsubplus;

import com.github.dockerjava.api.model.Ulimit;
import com.solace.test.integration.junit.jupiter.extension.pubsubplus.provider.container.SimpleContainerProvider;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.function.Supplier;

public class NetworkPubSubPlusContainerProvider extends SimpleContainerProvider {
	public static final Network DOCKER_NET = Network.newNetwork();
	public static final String DOCKER_NET_PUBSUB_ALIAS = "solace-pubsubplus";

	@Override
	public Supplier<PubSubPlusContainer> containerSupplier(ExtensionContext extensionContext) {
		return () -> new PubSubPlusContainer()
				.withNetwork(DOCKER_NET)
				.withNetworkAliases(DOCKER_NET_PUBSUB_ALIAS)

				// Workaround for PubSub+ ulimit requirements until we update to a fixed version of test support
				.withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
						.withUlimits(new Ulimit[] {new Ulimit("nofile", 2448, 1048576L)}))
				// Workaround for inadequate wait strategy until we update to a fixed version of test support
				.withExposedPorts(5550)  // Expose health check port
				.waitingFor(Wait.forHttp("/health-check/guaranteed-active")
						.forPort(5550)
						.forStatusCode(200)
						.withStartupTimeout(Duration.ofMinutes(5)));
	}
}
