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

package com.solace.source.connector;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPSessionStats;
import com.solacesystems.jcsmp.statistics.StatType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SolaceSourceTask extends SourceTask { // implements XMLMessageListener{

  private static final Logger log = LoggerFactory.getLogger(SolaceSourceTask.class);

  final JCSMPProperties properties = new JCSMPProperties();

  SolaceSourceConnectorConfig connectorConfig;
  BlockingQueue<BytesXMLMessage> ingressQueue 
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for any incoming message from PS+ topics and queue
  BlockingQueue<BytesXMLMessage> outstandingAckList 
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for Solace Flow messages
  String skafkaTopic;
  SolaceSourceTopicListener listener = null;
  SolaceSourceQueueConsumer consumer = null;
  boolean shuttingDown = false;

  private SolSessionCreate sessionRef = null;

  // private Class<?> cProcessor;
  private SolMessageProcessor processor;

  private static int BATCH_SIZE = 500;
  private int processed = 0;
  private int fmsgProcessed = 0;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {

    connectorConfig = new SolaceSourceConnectorConfig(props);
    skafkaTopic = connectorConfig.getString(SolaceSourceConstants.KAFKA_TOPIC);
    sessionRef = new SolSessionCreate(connectorConfig);
    sessionRef.configureSession();
    boolean connected = sessionRef.connectSession();
    if (!connected || sessionRef.getSession() == null) {
      log.info("============Failed to create JCSMPSession Session");
      stop();
    }
    log.info("======================JCSMPSession Connected");

    if (connectorConfig.getString(SolaceSourceConstants.SOL_TOPICS) != null) {
      listener = new SolaceSourceTopicListener(connectorConfig);
      boolean topicListenerStarted = listener.init(sessionRef.getSession(), ingressQueue);
      if (topicListenerStarted == false) {
        log.info("===============Failed to start topic consumer ... shutting down");
        stop();
      }
    }

    if (connectorConfig.getString(SolaceSourceConstants.SOl_QUEUE) != null) {
      consumer = new SolaceSourceQueueConsumer(connectorConfig);
      boolean queueConsumerStarted = consumer.init(sessionRef.getSession(), ingressQueue);
      if (queueConsumerStarted == false) {
        log.info("===============Failed to start queue consumer ... shutting down");
        stop();
      }
    }

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {

    /*
     * Return null if shutting down
     * Interrupt if waiting for messages (probably should return null anyway)
     * 
     */
      
    // Block waiting for a record to arrive or process in batches depending on the
    // number of records in array to process
    if (ingressQueue.size() == 0) {
        return null;
    }
    
    // There is at least one message to process
    List<SourceRecord> records = new ArrayList<>();
    int count = 0;
    int arraySize = ingressQueue.size();
    while (count < arraySize) {
      BytesXMLMessage msg = ingressQueue.take();
      processor = connectorConfig
          .getConfiguredInstance(SolaceSourceConstants
              .SOL_MESSAGE_PROCESSOR, SolMessageProcessor.class)
          .process(connectorConfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
      Collections.addAll(records, processor.getRecords(skafkaTopic));
      count++;
      processed++;
      if (msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT 
          || msg.getDeliveryMode() == DeliveryMode.PERSISTENT) {
        outstandingAckList.add(msg);
        fmsgProcessed++;
      }
    }

    if (fmsgProcessed > 0) {
      commit();
    }
    log.debug("Processed {} records in this batch.", processed);
    processed = 0;
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
    fmsgProcessed = 0;

  }

  @Override
  public void stop() {
    
    // TODO: 
    /*
     * shuttingDown = true
     * Waiting for all received messages to be acknowledged
     * Terminate listener / consumer
     * Terminate session
     * 
     */
    log.info("==================Shutting down Solace Source Connector");
    shuttingDown = true;
    if (fmsgProcessed > 0) {
      try {
        commit();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
    // TODO: Stats may be incomplete - REview!  
    JCSMPSession session = sessionRef.getSession();  
    if (session != null) {
      JCSMPSessionStats lastStats = session.getSessionStats();
      Enumeration<StatType> estats = StatType.elements();
      log.info("Final Statistics summary:");

      while (estats.hasMoreElements()) {
        StatType statName = estats.nextElement();
        System.out.println("\t" + statName.getLabel() + ": " + lastStats.getStat(statName));
      }
      log.info("\n");
    }
    
    boolean ok = true;
    if (listener != null) {
      ok &= listener.shutdown();
    }
    if (consumer != null) {
      ok &= consumer.shutdown();
    }
    if (sessionRef != null) {
      ok &= sessionRef.shutdown();
    }
    if (!(ok)) {
      log.info("Solace session failed to shutdown");
    }
    sessionRef = null; // At this point filling the ingress queue is stopped
  }

  // For testing only
  public JCSMPSession getSolSession() {
    return sessionRef.getSession();
  }
  
}
