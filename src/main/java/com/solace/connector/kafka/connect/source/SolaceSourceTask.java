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
import com.solacesystems.jcsmp.JCSMPException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolaceSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(SolaceSourceTask.class);

  SolaceSourceConnectorConfig connectorConfig;
  private SolSessionHandler solSessionHandler = null;
  BlockingQueue<BytesXMLMessage> ingressMessages
      = new LinkedBlockingQueue<>(); // LinkedBlockingQueue for any incoming message from PS+ topics and queue

  // Tracks correlation between SourceRecords and Solace messages for acknowledgment
  private final MessageTracker messageTracker = new MessageTracker();

  String skafkaTopic;
  SolaceSourceTopicListener topicListener = null;
  SolaceSourceQueueConsumer queueConsumer = null;
  private int spinTurns = 0;
  private volatile boolean shuttingDown = false;
  private final AtomicReference<JCSMPException> listenerExceptionReference = new AtomicReference<>();

  private SolMessageProcessorIF processor;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> props) {

    connectorConfig = createConnectorConfig(props);
    try {
      processor = connectorConfig.getConfiguredInstance(
          SolaceSourceConstants.SOL_MESSAGE_PROCESSOR, SolMessageProcessorIF.class);
    } catch (Exception e) {
      throw new ConnectException("Encountered exception in creating the message processor.", e);
    }
    skafkaTopic = connectorConfig.getString(SolaceSourceConstants.KAFKA_TOPIC);
    solSessionHandler = createSessionHandler(connectorConfig);
    try {
      solSessionHandler.configureSession();
      solSessionHandler.connectSession();
    } catch (JCSMPException e) {
      throw new ConnectException("Failed to create JCSMPSession", e);
    }
    log.info("================ JCSMPSession Connected");
    if (connectorConfig.getString(SolaceSourceConstants.SOL_TOPICS) != null) {
      topicListener = createTopicListener(connectorConfig, solSessionHandler);
      try {
        topicListener.init(this);
      } catch (JCSMPException e) {
        throw new ConnectException("Failed to start topic consumer", e);
      }
    }
    if (connectorConfig.getString(SolaceSourceConstants.SOL_QUEUE) != null) {
      queueConsumer = createQueueConsumer(connectorConfig, solSessionHandler);
      try {
        queueConsumer.init(this);
      } catch (JCSMPException e) {
        throw new ConnectException("Failed to start queue consumer", e);
      }
    }
  }

  // return null instead of empty collection is as per Connect docs
  @SuppressWarnings({"java:S1168", "PMD.ReturnEmptyCollectionRatherThanNull"})
  @Override
  public synchronized List<SourceRecord> poll() throws InterruptedException {
    JCSMPException listenerException = listenerExceptionReference.getAndSet(null);
    if (listenerException != null) {
      log.warn("Unrecoverable JCSMP listener exception detected", listenerException);
      throw new ConnectException("Message listener connection error to source on PubSub+ broker",
          listenerException);
    }
    if (shuttingDown || ingressMessages.isEmpty()) {
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
    int discarded = 0;
    int batchSize = ingressMessages.size(); // freeze num of messages to process in this call
    for (int processed = 0; processed < batchSize; processed++) {
      BytesXMLMessage msg = ingressMessages.take();
      try {
          processor.process(connectorConfig.getString(SolaceSourceConstants.SOL_KAFKA_MESSAGE_KEY), msg);
      } catch (Exception e) {
        if (connectorConfig.getBoolean(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_IGNORE_ERROR)) {
          log.warn("================ Encountered exception in message processing....discarded.", e);
          msg.ackMessage();
          discarded++;
          continue;
        } else {
          throw new ConnectException("Encountered exception in message processing", e);
        }
      }

      // Store correlation between SourceRecords and Solace message for later ACK in commitRecord()
      SourceRecord[] sourceRecords = processor.getRecords(skafkaTopic);
      if (sourceRecords == null || sourceRecords.length == 0) {
        // Message successfully processed but produced no records - ACK immediately
        // This prevents infinite redelivery of messages that legitimately produce no output
        log.debug("Message produced no records, ACKing immediately");
        msg.ackMessage();
        continue;
      }

      // Track records for later acknowledgment when framework calls commitRecord()
      messageTracker.track(msg, sourceRecords);
      Collections.addAll(records, sourceRecords);
    }
    log.debug("Processed {} records in this batch. Discarded {}", batchSize - discarded, discarded);
    return records;
  }

  /**
   * <p>Framework callback for individual record acknowledgment.
   * Called when the framework confirms a record was successfully processed
   * (sent to Kafka, filtered by transformation, or dropped with errors.tolerance=all).</p>
   *
   * <p>The framework controls when this is called based on producer callbacks and error handling.
   * </p>
   *
   * <p><b>Note:</b> A single BytesXMLMessage may produce multiple SourceRecords. We only
   * ACK the Solace message after ALL its SourceRecords have been committed.</p>
   *
   * @param sourceRecord the SourceRecord that was processed
   * @param metadata RecordMetadata from Kafka producer (null if filtered/dropped)
   */
  @Override
  public synchronized void commitRecord(SourceRecord sourceRecord, RecordMetadata metadata) {
    if (metadata != null) {
      log.debug("Source record {} written to Kafka at {}", sourceRecord.sourceOffset(), metadata);
    } else {
      log.debug("Source record {} was dropped by Kafka Connect", sourceRecord.sourceOffset());
    }

    BytesXMLMessage message = messageTracker.commitRecord(sourceRecord);
    if (message != null) { // All records for this message have been committed - ACK it

      // Always ACK when commitRecord() is called,
      // even if metadata=null (filtered or failed with errors.tolerance=all).
      // Framework calling this method means "I'm done with this record" - ACKing is part of the
      // expected contract.
      // See KIP-779: https://cwiki.apache.org/confluence/display/KAFKA/KIP-779%3A+Allow+Source+Tasks+to+Handle+Producer+Exceptions
      message.ackMessage();
      log.debug("ACKed Solace message (all records from this message processed by Kafka Connect)");
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

    // Clear unpolled messages from ingress queue
    // Note: These messages are intentionally NOT ACKed - they were received but never
    // polled, so Kafka never knew about them. They'll be redelivered on restart.
    ingressMessages.clear();

    // Clear message tracking data
    messageTracker.clear();
    log.info("PubSub+ Source Connector stopped");
  }

  // To access the message queue by the listeners
  public BlockingQueue<BytesXMLMessage> getIngressMessageQueue() {
    return ingressMessages;
  }

  // Used by listener to set exception condition
  public void setListenerException(JCSMPException listenerException) {
    JCSMPException existingException = listenerExceptionReference.getAndUpdate(
        prev -> prev != null ? prev : listenerException);

    if (existingException != null) {
      // There was already an exception recorded,
      // so we add this new one as suppressed to preserve the full context
      existingException.addSuppressed(listenerException);
    }
  }

  // exposed for testing
  SolaceSourceConnectorConfig createConnectorConfig(Map<String, String> props) {
    return new SolaceSourceConnectorConfig(props);
  }

  // exposed for testing
  SolSessionHandler createSessionHandler(SolaceSourceConnectorConfig config) {
    return new SolSessionHandler(config);
  }

  // exposed for testing
  SolaceSourceTopicListener createTopicListener(SolaceSourceConnectorConfig config,
                                                  SolSessionHandler handler) {
    return new SolaceSourceTopicListener(config, handler);
  }

  // exposed for testing
  SolaceSourceQueueConsumer createQueueConsumer(SolaceSourceConnectorConfig config,
                                                  SolSessionHandler handler) {
    return new SolaceSourceQueueConsumer(config, handler);
  }

  // exposed for testing
  MessageTracker getMessageTracker() {
    return messageTracker;
  }

  /**
   * Tracks the correlation between {@link SourceRecord}s and their originating
   * {@link BytesXMLMessage}s to enable acknowledgment after framework commit.
   *
   * <p>A single Solace message may produce multiple SourceRecords. All records from the
   * same message share a {@link MessageToRecordsContext} that groups the message with its
   * pending records. When all records are committed, the shared pending set becomes empty,
   * indicating the message is ready for acknowledgment.
   *
   * <p><b>Thread Safety:</b> Methods are NOT thread-safe. Caller must provide external
   * synchronization (SolaceSourceTask's synchronized methods provide this).
   */
  static final class MessageTracker {

    /**
     * Context object that groups a Solace message with its pending SourceRecords.
     * Multiple SourceRecords from the same message share the same context instance,
     * enabling automatic tracking of when all records are committed.
     */
    static final class MessageToRecordsContext {
      final BytesXMLMessage message;
      final Set<SourceRecord> pendingRecords;

      MessageToRecordsContext(BytesXMLMessage message, Set<SourceRecord> pendingRecords) {
        this.message = message;
        this.pendingRecords = pendingRecords;
      }
    }

    // Use IdentityHashMap: Kafka Connect framework guarantees passing the same SourceRecord instance
    // to commitRecord() that was returned from poll() (verified in AbstractWorkerSourceTask.sendRecords).
    // Must use identity-based equality because sample processors set sourcePartition/sourceOffset to null,
    // which means SourceRecords with identical content would be value-equal and cause HashMap collisions,
    // leading to message tracking corruption and ACK failures.
    // All SourceRecords from the same message share the same MessageToRecordsContext instance.
    private final Map<SourceRecord, MessageToRecordsContext> recordToContext = new IdentityHashMap<>();

    private static final Logger log = LoggerFactory.getLogger(MessageTracker.class);

    private MessageTracker() {}

    /**
     * Track a Solace message and its associated SourceRecords. Creates a shared context
     * that all records point to, enabling automatic tracking of completion.
     *
     * @param message the BytesXMLMessage that produced the records
     * @param records the SourceRecords produced from this message
     */
    void track(BytesXMLMessage message, SourceRecord[] records) {
      // Use identity-based Set: Must match IdentityHashMap semantics for recordToContext.
      // When commitRecord() removes from this Set, it must use instance identity, not value equality.
      Set<SourceRecord> pendingRecords = Collections.newSetFromMap(new IdentityHashMap<>(records.length));
      Collections.addAll(pendingRecords, records);

      // Create single context instance shared by all records from this message
      MessageToRecordsContext context = new MessageToRecordsContext(message, pendingRecords);

      // All records point to the same context instance
      for (SourceRecord sourceRecord : records) {
        recordToContext.put(sourceRecord, context);
      }
    }

    /**
     * Mark a SourceRecord as committed by the framework. Returns the BytesXMLMessage if ALL its
     * records are now committed (ready to ACK), otherwise returns null.
     *
     * @param sourceRecord the SourceRecord that was committed
     * @return the BytesXMLMessage to ACK, or null if more records are pending
     */
    BytesXMLMessage commitRecord(SourceRecord sourceRecord) {
      MessageToRecordsContext context = recordToContext.remove(sourceRecord);
      if (context == null) {
        log.debug("SourceRecord already processed or unknown, skipping");
        return null;
      }

      // Remove from shared pendingRecords set
      if (!context.pendingRecords.remove(sourceRecord)) {
        // Defensive check: should never happen if track() maintains consistency - indicates a coding bug
        log.error("Inconsistent state: SourceRecord found in map but not in context's pending set");
        return null;
      }

      if (!context.pendingRecords.isEmpty()) {
        log.trace("Solace message has {} more record(s) pending commit", context.pendingRecords.size());
        return null;
      }

      log.trace("All SourceRecords for a Solace message have been committed, ready to ACK");
      return context.message;
    }

    /**
     * Clear all tracking data. Called during shutdown.
     */
    void clear() {
      // Count unique messages (using Set since multiple records may share same context)
      int pendingCount = (int) recordToContext.values().stream()
          .map(ctx -> ctx.message)
          .distinct()
          .count();

      if (pendingCount > 0) {
        log.debug("Stopping with {} messages pending framework acknowledgment", pendingCount);
      }

      recordToContext.clear();
    }

    // exposed for testing
    Map<SourceRecord, MessageToRecordsContext> getRecordToContextMap() {
      return recordToContext;
    }
  }
}
