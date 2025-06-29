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

import com.solacesystems.jcsmp.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;


public class SolaceSourceTask extends SourceTask { // implements XMLMessageListener{

  private static final Logger log = LoggerFactory.getLogger(SolaceSourceTask.class);

  SolaceSourceConnectorConfig connectorConfig;
  private SolSessionHandler solSessionHandler = null;
  BlockingQueue<BytesXMLMessage> ingressMessages
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for any incoming message from PS+ topics and queue
  Map<BytesXMLMessage, Integer> pendingAcks = new HashMap<>(); // Pending acks for solace message with number of created record
  Map<SourceRecord, BytesXMLMessage> recordToMessage = new HashMap<>(); // Map record to solace message

  //Scheduled buffer for acks,
  private final List<BytesXMLMessage> ackBuffer = new ArrayList<>(); // Buffer for acknowledgments
  private int ackBufferSize; // Maximum buffer size before flush

  private long lastMessageAddedTime = System.currentTimeMillis(); // Track time of last addition to the buffer
  private final ScheduledExecutorService flushScheduler = Executors.newScheduledThreadPool(1);

  String skafkaTopic;
  SolaceSourceTopicListener topicListener = null;
  SolaceSourceQueueConsumer queueConsumer = null;
  private int spinTurns = 0;
  private volatile boolean shuttingDown = false;
  private JCSMPException listenerException = null;

  // private Class<?> cProcessor;
  private SolMessageProcessorIF processor;
  private int ackTimeout;

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
      throw new ConnectException("Encountered exception in creating the message processor.", e);
    }
    skafkaTopic = connectorConfig.getString(SolaceSourceConstants.KAFKA_TOPIC);
    solSessionHandler = new SolSessionHandler(connectorConfig);
    try {
      solSessionHandler.configureSession();
      solSessionHandler.connectSession();
    } catch (JCSMPException e) {
      throw new ConnectException("Failed to create JCSMPSession", e);
    }
    log.info("================ JCSMPSession Connected");
    if (connectorConfig.getString(SolaceSourceConstants.SOL_TOPICS) != null) {
      topicListener = new SolaceSourceTopicListener(connectorConfig, solSessionHandler);
      try {
        topicListener.init(this);
      } catch (JCSMPException e) {
        throw new ConnectException("Failed to start topic consumer", e);
      }
    }
    if (connectorConfig.getString(SolaceSourceConstants.SOL_QUEUE) != null) {
      queueConsumer = new SolaceSourceQueueConsumer(connectorConfig, solSessionHandler);
      try {
        queueConsumer.init(this);
      } catch (JCSMPException e) {
        throw new ConnectException("Failed to start queue consumer", e);
      }
    }

    setupScheduler();
  }

  private void setupScheduler() {
    //Align scheduler and buffer with underlying flowHandle.startAckTimer() and UnackedMessageList2.thresholdCount
    int subWinSz = (Integer)solSessionHandler.getSession().getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);
    int subThreshold = (Integer)solSessionHandler.getSession().getProperty(JCSMPProperties.SUB_ACK_WINDOW_THRESHOLD);
    ackBufferSize = subWinSz * subThreshold / 100;

    ackTimeout = (Integer)solSessionHandler.getSession().getProperty(JCSMPProperties.SUB_ACK_TIME);

    // Start the scheduled task to periodically flush the buffer
    flushScheduler.scheduleAtFixedRate(() -> {
      try {
        flushAckBufferIfNeeded();
      } catch (Exception e) {
        log.error("Error during scheduled ack buffer flush", e);
      }
    }, ackTimeout, ackTimeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized List<SourceRecord> poll() throws InterruptedException {

    List<SourceRecord> records = null;
    
    if (listenerException != null) {
      log.warn("Unrecoverable JCSMP listener exception detected");
      JCSMPException exceptionToPropagate = listenerException;
      listenerException = null;
      throw new ConnectException("Message listener connection error to source on PubSub+ broker", exceptionToPropagate);
    }
    if (shuttingDown || ingressMessages.size() == 0) {
      spinTurns++;
      if (spinTurns > 100) {
        spinTurns = 0;
        Thread.sleep(1);
      }
      return records;  // Nothing to do, return control (returns null)
    }
    // There is at least one message to process
    spinTurns = 0; // init spinTurns again
    records = new ArrayList<>();
    int processedInThisBatch;
    int discarded = 0;
    int arraySize = ingressMessages.size();
    for (processedInThisBatch = 0; processedInThisBatch < arraySize; processedInThisBatch++) {
      BytesXMLMessage msg = ingressMessages.take();
      try {
          processor.process(connectorConfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
      } catch (Exception e) {
        if (connectorConfig.getBoolean(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_IGNORE_ERROR)) {
          log.warn("================ Encountered exception in message processing....discarded.", e);
          msg.ackMessage(); // Effective discard solace message
          discarded++;
          continue;
        } else {
          throw new ConnectException("Encountered exception in message processing", e);
        }
      }
      SourceRecord[] processorRecords = processor.getRecords(skafkaTopic);
      Collections.addAll(records, processorRecords);
      scheduleForAck(msg, processorRecords);
    }
    log.debug("Processed {} records in this batch. Discarded {}", processedInThisBatch - discarded, discarded);
    return records;
  }

  private synchronized void scheduleForAck(BytesXMLMessage msg, SourceRecord[] processorRecords) {
    if (msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT
            || msg.getDeliveryMode() == DeliveryMode.PERSISTENT) {
      for (SourceRecord processorRecord : processorRecords) {
        recordToMessage.put(processorRecord, msg); // Map each record to solace message id
      }
      pendingAcks.put(msg, processorRecords.length); // enqueue messages received from guaranteed messaging endpoint for later ack
    }
  }

  @Override
  public synchronized void commitRecord(SourceRecord record, RecordMetadata metadata) {
    BytesXMLMessage msg = recordToMessage.remove(record);
    if (msg == null) {
      log.error("Unable to find message for record {}", record); // Shouldn't happens
      return;
    }

    if (!pendingAcks.containsKey(msg)) {
      log.error("Unable to find message counter for message {}", msg); // Shouldn't happens
    }

    pendingAcks.computeIfPresent(msg, (k, o) -> o > 1 ? --o : null);// Reduce counter of records per message, remove on last

    if (!pendingAcks.containsKey(msg)) {// Last record was commited in the group
      ackBuffer.add(msg);
      lastMessageAddedTime = System.currentTimeMillis(); // Update last message addition time

      // Flush the buffer if it reaches the maximum buffer size
      if (ackBuffer.size() >= ackBufferSize) {
        flushAckBuffer();
      }
      log.debug("Buffer ack for message {}", msg);
    }
  }

  private synchronized void flushAckBufferIfNeeded() {
    long currentTime = System.currentTimeMillis();
    if (!ackBuffer.isEmpty() && (currentTime - lastMessageAddedTime) >= ackTimeout) {
      flushAckBuffer();
    }
  }

  private synchronized void flushAckBuffer() {
    for (BytesXMLMessage msg : ackBuffer) {
      msg.ackMessage();
      log.debug("Acknowledged message {}", msg);
    }

    ackBuffer.clear(); // Clear the buffer after acknowledgment
    log.debug("Flushed acknowledgment buffer");
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
    recordToMessage.clear();

    if (!pendingAcks.isEmpty()) {
      log.warn("Potential duplicates might be spotted");
      pendingAcks.forEach((s, m) -> log.warn("Dup: {}", m));
      pendingAcks.clear();
    }

    // Flush remaining messages in the buffer
    flushAckBuffer();
    if (!flushScheduler.isShutdown()) {
      flushScheduler.shutdown();
      try {
        if (!flushScheduler.awaitTermination(500, TimeUnit.MILLISECONDS)) {
          flushScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        flushScheduler.shutdownNow();
      }
    }

    log.info("PubSub+ Source Connector stopped");
  }

  // To access the message queue by the listeners
  public BlockingQueue<BytesXMLMessage> getIngressMessageQueue() {
    return ingressMessages;
  }

  // Used by listener to set exception condition
  public void setListenerException(JCSMPException listenerException) {
    this.listenerException = listenerException;
  }
  
  // For testing only
  public JCSMPSession getSolSession() {
    return solSessionHandler.getSession();
  }

}
