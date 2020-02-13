package com.solace.messaging;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.solace.testcontainer.wait.startegy.WaitExtension;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;

import java.io.File;
import java.time.Duration;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.wait.strategy.Wait;

@Testcontainers
public interface MessagingServiceFullLocalSetup {

  public static final String PUBSUB_TAG = "9.4.0.24";
  public static final String PUBSUB_HOSTNAME = "solbroker";
  public static final String PUBSUB_NETWORK_NAME = "solace_msg_network";
  public static final String FULL_DOCKER_COMPOSE_FILE_PATH = "src/integrationTest/resources/";
  public static final String[] SERVICES = new String[]{"solbroker"};
  public static final long MAX_STARTUP_TIMEOUT_MSEC = 120000l;
  public static final String DIRECT_MESSAGING_HTTP_HEALTH_CHECK_URI = "/health-check/direct-active";
  public static final int DIRECT_MESSAGING_HTTP_HEALTH_CHECK_PORT = 5550;
  public static final String GUARANTEED_MESSAGING_HTTP_HEALTH_CHECK_URI = "/health-check/guaranteed-active";
  public static final int GUARANTEED_MESSAGING_HTTP_HEALTH_CHECK_PORT = 5550;

  public static final String CONNECTORSOURCE = "build/distributions/pubsubplus-connector-kafka-source.zip";
  public static final String CONNECTORDESTINATION = "src/integrationTest/resources/";
  public static final String CONNECTORPROPERTIESFILE = CONNECTORDESTINATION+"pubsubplus-connector-kafka-source/etc/solace.properties";
  
  public static final String SOL_ADMINUSER_NAME = "default";
  public static final String SOL_ADMINUSER_PW = "default";
  public static final String SOL_VPN = "default";
  public static final String KAFKA_TOPIC = "kafka-test-topic";
  public static final String SOL_TOPICS = "pubsubplus-test-topic";
  public static final String CONN_MSGPROC_CLASS = "com.solace.source.connector.msgprocessors.SolSampleSimpleMessageProcessor";
  public static final String CONN_KAFKA_MSGKEY = "DESTINATION";
  
  
  @Container
  public static final DockerComposeContainer COMPOSE_CONTAINER_PUBSUBPLUS =
      new DockerComposeContainer(
          new File(FULL_DOCKER_COMPOSE_FILE_PATH + "docker-compose-solace.yml"))
          .withEnv("PUBSUB_NETWORK_NAME", PUBSUB_NETWORK_NAME)
          .withEnv("PUBSUB_HOSTNAME", PUBSUB_HOSTNAME)
          .withEnv("PUBSUB_TAG", PUBSUB_TAG)
          .withServices(SERVICES)
          .withLocalCompose(true)
          .withPull(false)
          .waitingFor("solbroker_1",
              WaitExtension.forHttp("/")
                  .withStartupTimeout(Duration.ofMillis(MAX_STARTUP_TIMEOUT_MSEC)))
          .waitingFor("solbroker_1",
              WaitExtension.forHttp(DIRECT_MESSAGING_HTTP_HEALTH_CHECK_URI,
                  DIRECT_MESSAGING_HTTP_HEALTH_CHECK_PORT)
                  .withStartupTimeout(Duration.ofMillis(60000)))
          .waitingFor("solbroker_1",
              WaitExtension.forHttp(GUARANTEED_MESSAGING_HTTP_HEALTH_CHECK_URI,
                  GUARANTEED_MESSAGING_HTTP_HEALTH_CHECK_PORT)
                  .withStartupTimeout(Duration.ofMillis(10000)));

  @Container
  public static final DockerComposeContainer COMPOSE_CONTAINER_KAFKA =
        new DockerComposeContainer(
            new File(FULL_DOCKER_COMPOSE_FILE_PATH + "docker-compose-kafka.yml"))
            .withEnv("KAFKA_TOPIC", KAFKA_TOPIC)
            .withLocalCompose(true)
            .waitingFor("schema-registry_1",
                Wait.forHttp("/subjects").forStatusCode(200));

  @BeforeAll
  static void checkContainer() {
    String host = COMPOSE_CONTAINER_PUBSUBPLUS.getServiceHost("solbroker_1", 8080);
    assertNotNull(host);
  }
  
  
  @BeforeAll
  static void setupConnector() {
	  try {
	      // Copy to resources
		  ZipFile zipFile = new ZipFile(CONNECTORSOURCE);
	      zipFile.extractAll(CONNECTORDESTINATION);
	      
	      // Configure connector params
	      Parameters params = new Parameters();
	      FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
	          new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
	          .configure(params.properties()
	              .setFileName(CONNECTORPROPERTIESFILE));
	      Configuration config = builder.getConfiguration();
	      config.setProperty("sol.host", "tcp://" + COMPOSE_CONTAINER_PUBSUBPLUS.getServiceHost("solbroker_1", 55555) + ":55555");
	      config.setProperty("sol.username", SOL_ADMINUSER_NAME);
	      config.setProperty("sol.password", SOL_ADMINUSER_PW);
	      config.setProperty("sol.vpn_name", SOL_VPN);
	      config.setProperty("kafka.topic", KAFKA_TOPIC);
	      config.setProperty("sol.topics", SOL_TOPICS);
	      config.setProperty("sol.message_processor_class", CONN_MSGPROC_CLASS);
	      config.setProperty("sol.kafka_message_key", CONN_KAFKA_MSGKEY);

	      builder.save();
	      
  	 } catch (ZipException e) {
  	     e.printStackTrace();
  	 } catch (ConfigurationException e) {
  		e.printStackTrace();
   	 }

  }

}

