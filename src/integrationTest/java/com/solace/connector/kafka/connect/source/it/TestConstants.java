package com.solace.connector.kafka.connect.source.it;

import com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor;

public interface TestConstants {
    String UNZIPPEDCONNECTORDESTINATION = "src/integrationTest/resources";
    String CONNECTORJSONPROPERTIESFILE = "etc/solace_source_properties.json";

    String SOL_TOPICS = "pubsubplus-test-topic";
    String SOL_QUEUE = "pubsubplus-test-queue";
    String CONN_MSGPROC_CLASS = SolSampleSimpleMessageProcessor.class.getName();
    String CONN_KAFKA_MSGKEY = "DESTINATION";

}
