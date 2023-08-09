package com.solace.connector.kafka.connect.source.it.util.extensions.pubsubplus.pubsubplus;

import com.solace.test.integration.junit.jupiter.extension.pubsubplus.provider.container.SimpleContainerProvider;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.Network;

import java.util.function.Supplier;

public class NetworkPubSubPlusContainerProvider extends SimpleContainerProvider {
	public static final Network DOCKER_NET = Network.newNetwork();
	public static final String DOCKER_NET_PUBSUB_ALIAS = "solace-pubsubplus";

	@Override
	public Supplier<PubSubPlusContainer> containerSupplier(ExtensionContext extensionContext) {
		return () -> new PubSubPlusContainer()
				.withNetwork(DOCKER_NET)
				.withNetworkAliases(DOCKER_NET_PUBSUB_ALIAS);
	}
}
