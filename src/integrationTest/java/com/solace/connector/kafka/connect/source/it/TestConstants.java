package com.solace.connector.kafka.connect.source.it;

public interface TestConstants {

    public static final String PUBSUB_TAG = "latest";
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
    public static final String UNZIPPEDCONNECTORDESTINATION = "src/integrationTest/resources";
    public static final String CONNECTORPROPERTIESFILE = "etc/solace_source.properties";
    public static final String CONNECTORJSONPROPERTIESFILE = "etc/solace_source_properties.json";
    
    public static final String SOL_ADMINUSER_NAME = "default";
    public static final String SOL_ADMINUSER_PW = "default";
    public static final String SOL_VPN = "default";
    public static final String KAFKA_SOURCE_TOPIC = "kafka-source-test-topic";
    public static final String SOL_TOPICS = "pubsubplus-test-topic";
    public static final String SOL_QUEUE = "pubsubplus-test-queue";
    public static final String CONN_MSGPROC_CLASS = "com.solace.source.connector.msgprocessors.SolSampleSimpleMessageProcessor";
    public static final String CONN_KAFKA_MSGKEY = "DESTINATION";
    
}
