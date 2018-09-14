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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SolaceSourceTask extends SourceTask { // implements XMLMessageListener{

  private static final Logger log = LoggerFactory.getLogger(SolaceSourceTask.class);

  final JCSMPProperties properties = new JCSMPProperties();

  SolaceSourceConfig sconfig;
  SolaceSourceTopicListener listener;
  SolaceSourceQueueConsumer consumer;
  BlockingQueue<BytesXMLMessage> squeue 
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for Solace Topic
  // subscription messages
  BlockingQueue<BytesXMLMessage> gqueue 
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for Solace Flow messages
  // from Solace Queue
  String skafkaTopic;
  boolean topicListenStarted = true;
  boolean queueConsumerStarted = true;

  private SolSessionCreate sessionRef;
  private JCSMPSession session;

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

    sconfig = new SolaceSourceConfig(props);
    skafkaTopic = sconfig.getString(SolaceSourceConstants.KAFKA_TOPIC);

    sessionRef = new SolSessionCreate(sconfig);
    sessionRef.configureSession();
    boolean connected = sessionRef.connectSession();
    if (!connected) {
      log.info("============Failed to create Solace Session");
      stop();
    }
    session = sessionRef.getSession();
    if (session != null) {
      log.info("======================JCSMPSession Connected");
    } else {
      log.info("======================Failed to create JCSMPSession");
      stop();
    }

    if (sconfig.getString(SolaceSourceConstants.SOL_TOPICS) != null) {
      listener = new SolaceSourceTopicListener(sconfig, squeue);
      topicListenStarted = listener.init(session);
      if (topicListenStarted == false) {
        log.info("===============Failed to start topic consumer ... shutting down");
        stop();
      }
    }

    if (sconfig.getString(SolaceSourceConstants.SOl_QUEUE) != null) {
      consumer = new SolaceSourceQueueConsumer(sconfig, squeue);
      queueConsumerStarted = consumer.init(session);
      if (queueConsumerStarted == false) {
        log.info("===============Failed to start queue consumer ... shutting down");
        stop();
      }
    }

  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {

    List<SourceRecord> records = new ArrayList<>();
    int arraySize = squeue.size();

    // Block waiting for a record to arrive or process in batches depending on the
    // number of records in array to process
    if (squeue.size() == 0) {
      BytesXMLMessage msg = squeue.take(); // Blocks here until there is a message
      processor = sconfig.getConfiguredInstance(SolaceSourceConstants
          .SOL_MESSAGE_PROCESSOR, SolMessageProcessor.class)
          .process(sconfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
      Collections.addAll(records, processor.getRecords(skafkaTopic));
      processed++;
      if (msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT 
          || msg.getDeliveryMode() == DeliveryMode.PERSISTENT) {
        gqueue.add(msg);
        fmsgProcessed++;
      }

    } else if (squeue.size() < BATCH_SIZE) {
      int count = 0;
      arraySize = squeue.size();
      while (count < arraySize) {
        BytesXMLMessage msg = squeue.take();
        processor = sconfig
            .getConfiguredInstance(SolaceSourceConstants
                .SOL_MESSAGE_PROCESSOR, SolMessageProcessor.class)
            .process(sconfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
        Collections.addAll(records, processor.getRecords(skafkaTopic));
        count++;
        processed++;
        if (msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT 
            || msg.getDeliveryMode() == DeliveryMode.PERSISTENT) {
          gqueue.add(msg);
          fmsgProcessed++;
        }
      }
    } else if (squeue.size() >= BATCH_SIZE) {
      int count = 0;
      int currentLoad = squeue.size();
      while (count < currentLoad) {
        BytesXMLMessage msg = squeue.take();
        processor = sconfig
            .getConfiguredInstance(SolaceSourceConstants
                .SOL_MESSAGE_PROCESSOR, SolMessageProcessor.class)
            .process(sconfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
        Collections.addAll(records, processor.getRecords(skafkaTopic));
        count++;
        processed++;
        if (msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT 
            || msg.getDeliveryMode() == DeliveryMode.PERSISTENT) {
          gqueue.add(msg);
          fmsgProcessed++;
        }
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
   * Kakfa Connect method that write records to disk.
   */
  public synchronized void commit() throws InterruptedException {
    log.trace("Committing records");
    int currentLoad = gqueue.size();
    int count = 0;
    while (count != currentLoad) {
      gqueue.take().ackMessage();
      count++;
    }
    fmsgProcessed = 0;

  }

  @Override
  public void stop() {
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
    log.info("==================Shutting down Solace Source Connector");
    if (listener != null) {
      ok = listener.shutdown();
    }
    if (consumer != null) {
      ok = consumer.shutdown();
    }
    if (session != null) {
      ok = sessionRef.shutdown();
    }
    if (!(ok)) {
      log.info("Solace session failed to shutdown");
    }

  }

}
