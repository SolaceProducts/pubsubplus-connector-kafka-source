/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.connector.kafka.connect.source;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SolaceSourceTask extends SourceTask { // implements XMLMessageListener{

  private static final Logger log = LoggerFactory.getLogger(SolaceSourceTask.class);

  final JCSMPProperties properties = new JCSMPProperties();

  SolaceSourceConnectorConfig connectorConfig;
  private SolSessionHandler solSessionHandler = null;
  BlockingQueue<BytesXMLMessage> ingressMessages 
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for any incoming message from PS+ topics and queue
  BlockingQueue<BytesXMLMessage> outstandingAckList 
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for Solace Flow messages
  String skafkaTopic;
  SolaceSourceTopicListener topicListener = null;
  SolaceSourceQueueConsumer queueConsumer = null;
  private int spinTurns = 0;
  private volatile boolean shuttingDown = false;

  // private Class<?> cProcessor;
  private SolMessageProcessorIF processor;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {

    connectorConfig = new SolaceSourceConnectorConfig(props);
    try {
      processor = connectorConfig
          .getConfiguredInstance(SolaceSourceConstants
              .SOL_MESSAGE_PROCESSOR, SolMessageProcessorIF.class);
    } catch (Exception e) {
      log.info(
          "================ Encountered exception in creating the message processor."
          + " Cause: {}, Stacktrace: {} ",
          e.getCause(), e.getStackTrace());
    }
    skafkaTopic = connectorConfig.getString(SolaceSourceConstants.KAFKA_TOPIC);
    solSessionHandler = new SolSessionHandler(connectorConfig);
    try {
      solSessionHandler.configureSession();
      solSessionHandler.connectSession();
    } catch (JCSMPException e) {
      log.info("Received Solace exception {}, with the "
          + "following: {} ", e.getCause(), e.getStackTrace());
      log.info("================ Failed to create JCSMPSession Session");
      stop();
    }
    log.info("================ JCSMPSession Connected");
    if (connectorConfig.getString(SolaceSourceConstants.SOL_TOPICS) != null) {
      topicListener = new SolaceSourceTopicListener(connectorConfig, solSessionHandler);
      if (!topicListener.init(ingressMessages)) {
        log.info("================ Failed to start topic consumer ... shutting down");
        stop();
      }
    }
    if (connectorConfig.getString(SolaceSourceConstants.SOL_QUEUE) != null) {
      queueConsumer = new SolaceSourceQueueConsumer(connectorConfig, solSessionHandler);
      if (!queueConsumer.init(ingressMessages)) {
        log.info("================ Failed to start queue consumer ... shutting down");
        stop();
      }
    }
  }

  @Override
  public synchronized List<SourceRecord> poll() throws InterruptedException {

    if (shuttingDown || ingressMessages.size() == 0) {
      spinTurns++;
      if (spinTurns > 100) {
        spinTurns = 0;
        Thread.sleep(1);
      }
      return null;  // Nothing to do, return control
    }
    // There is at least one message to process
    spinTurns = 0; // init spinTurns again
    List<SourceRecord> records = new ArrayList<>();
    int processedInIhisBatch = 0;
    int count = 0;
    int arraySize = ingressMessages.size();
    while (count < arraySize) {
      BytesXMLMessage msg = ingressMessages.take();
      try {
          processor.process(connectorConfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
      } catch (Exception e) {
        log.info(
            "================ Encountered exception in message processing....discarded."
            + " Cause: {}, Stacktrace: {} ",
            e.getCause(), e.getStackTrace());
      }
      Collections.addAll(records, processor.getRecords(skafkaTopic));
      count++;
      processedInIhisBatch++;
      if (msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT 
          || msg.getDeliveryMode() == DeliveryMode.PERSISTENT) {
        outstandingAckList.add(msg);  // enqueue messages received from guaranteed messaging endpoint for later ack 
      }
    }
    log.debug("Processed {} records in this batch.", processedInIhisBatch);
    return records;
  }

  /**
   * Kafka Connect method that write records to disk.
   */
  public synchronized void commit() throws InterruptedException {
    log.trace("Committing records");
    int currentLoad = outstandingAckList.size();
    int count = 0;
    while (count != currentLoad) {
      outstandingAckList.take().ackMessage();
      count++;
    }
  }

  @Override
  public synchronized void stop() {
    log.info("================ Shutting down PubSub+ Source Connector");
    shuttingDown = true;
    if (topicListener != null) {
      topicListener.shutdown();
    }
    if (queueConsumer != null) {
      queueConsumer.shutdown();
    }
    if (solSessionHandler != null) {
      log.info("Final Statistics summary:\n");
      solSessionHandler.printStats();
      solSessionHandler.shutdown();
    }
    solSessionHandler = null; // At this point filling the ingress queue is stopped
    ingressMessages.clear();  // Remove all remaining ingressed messages, these will be no longer imported to Kafka
    log.info("PubSub+ Source Connector stopped");
  }

  // For testing only
  public JCSMPSession getSolSession() {
    return solSessionHandler.getSession();
  }
  
}
