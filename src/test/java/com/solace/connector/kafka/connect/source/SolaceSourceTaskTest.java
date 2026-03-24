package com.solace.connector.kafka.connect.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.solace.connector.kafka.connect.source.SolaceSourceTask.MessageTracker;
import com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SolaceSourceTaskTest {

  private final TestSolaceSourceTask sourceTask = new TestSolaceSourceTask();

  @AfterEach
  void tearDown() {
    sourceTask.stop();
  }

  @Test
  void testNoProvidedMessageProcessor() {
    sourceTask.doNotUseMockProcessor();
    Map<String, String> props = Collections.emptyMap();
    assertThatThrownBy(() -> sourceTask.start(props))
        .isInstanceOf(ConnectException.class)
        .hasMessageContaining("Encountered exception in creating the message processor.")
        .hasCauseInstanceOf(KafkaException.class)
        .cause()
        .hasMessageContaining("Could not find a public no-argument constructor for "
            + SolMessageProcessorIF.class.getName());
  }

  @Test
  void testFailSessionConnect() {
    Map<String, String> props = new HashMap<>();
    props.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR,
        SolSampleSimpleMessageProcessor.class.getName());

    SolaceSourceTask realSourceTask = new SolaceSourceTask();
    try {
      assertThatThrownBy(() -> realSourceTask.start(props))
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("Failed to create JCSMPSession")
          .hasCauseInstanceOf(JCSMPException.class)
          .cause()
          .hasMessageContaining("Null value was passed in for property (host)");
    } finally {
      realSourceTask.stop();
    }
  }

  @Test
  void testCommitRecord_SingleRecordMessage_AcksImmediately() throws Exception {
    sourceTask.start(createTestProps("test-topic"));

    SourceRecord sourceRecord = createSourceRecord("test-topic", "test-value");
    when(sourceTask.getMockProcessor().getRecords(anyString()))
        .thenReturn(new SourceRecord[]{sourceRecord});

    BytesXMLMessage smfMessage = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
    sourceTask.getIngressMessageQueue().add(smfMessage);

    assertThat(sourceTask.poll())
        .isNotNull()
        .singleElement()
        .isEqualTo(sourceRecord);
    verify(smfMessage, never()).ackMessage();

    sourceTask.commitRecord(sourceRecord, null);

    verify(smfMessage).ackMessage();
    assertThat(sourceTask.getMessageTracker()).satisfies(assertMessageTrackerEmpty());
  }

  @CartesianTest
  void testCommitRecord_MultipleRecordMessage_AcksAfterAllCommitted(
      @CartesianTest.Values(ints = {2, 3, 5}) int recordCount,
      @CartesianTest.Enum(CommitOrder.class) CommitOrder order) throws Exception {
    sourceTask.start(createTestProps("test-topic"));

    SourceRecord[] records = IntStream.range(0, recordCount)
        .mapToObj(i -> createSourceRecord("topic", "value" + i))
        .toArray(SourceRecord[]::new);
    when(sourceTask.getMockProcessor().getRecords(anyString())).thenReturn(records);

    BytesXMLMessage smfMessage = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
    sourceTask.getIngressMessageQueue().add(smfMessage);

    assertThat(sourceTask.poll()).isNotNull().hasSize(recordCount);
    verify(smfMessage, never()).ackMessage();

    int[] commitOrder = order.generateCommitOrder(recordCount);

    for (int i = 0; i < recordCount - 1; i++) {
      sourceTask.commitRecord(records[commitOrder[i]], null);
      verify(smfMessage, never()).ackMessage();
    }

    sourceTask.commitRecord(records[commitOrder[recordCount - 1]], null);
    verify(smfMessage).ackMessage();
    assertThat(sourceTask.getMessageTracker()).satisfies(assertMessageTrackerEmpty());
  }

  @Test
  void testCommitRecord_UnknownRecord_NoAck() {
    sourceTask.start(createTestProps(null));

    SourceRecord unknownRecord = createSourceRecord("unknown-topic", "unknown-value");

    sourceTask.commitRecord(unknownRecord, null);

    assertThat(sourceTask.getMessageTracker()).satisfies(assertMessageTrackerEmpty());
  }

  // ========== poll() Tests ==========

  @ParameterizedTest
  @EnumSource(RecordsScenario.class)
  void testPoll_NoRecordsFromProcessor_AcksImmediately(RecordsScenario scenario) throws Exception {
    sourceTask.start(createTestProps("test-topic"));

    when(sourceTask.getMockProcessor().getRecords(anyString())).thenReturn(scenario.getRecords());

    BytesXMLMessage smfMessage = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
    sourceTask.getIngressMessageQueue().add(smfMessage);

    assertThat(sourceTask.poll()).isNotNull().isEmpty();
    verify(smfMessage).ackMessage();
    assertThat(sourceTask.getMessageTracker()).satisfies(assertMessageTrackerEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 5})
  void testPoll_NormalFlow_TracksRecords(int recordCount) throws Exception {
    sourceTask.start(createTestProps("test-topic"));

    SourceRecord[] records = IntStream.range(0, recordCount)
        .mapToObj(i -> createSourceRecord("test-topic", "value" + i))
        .toArray(SourceRecord[]::new);
    when(sourceTask.getMockProcessor().getRecords(anyString())).thenReturn(records);

    BytesXMLMessage smfMessage = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
    sourceTask.getIngressMessageQueue().add(smfMessage);

    assertThat(sourceTask.poll())
        .isNotNull()
        .hasSize(recordCount)
        .containsExactlyInAnyOrder(records);
    verify(smfMessage, never()).ackMessage();

    MessageTracker tracker = sourceTask.getMessageTracker();
    assertThat(tracker.getRecordToMessageMap()).hasSize(recordCount);
    assertThat(tracker.getMessagePendingRecords()).hasSize(1);
    assertThat(tracker.getMessagePendingRecords().get(smfMessage)).isNotNull().hasSize(recordCount);
  }

  @Test
  void testPoll_ProcessorThrowsException_WithIgnoreError_AcksAndContinues() throws Exception {
    Map<String, String> props = createTestProps("test-topic");
    props.put(SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_IGNORE_ERROR, "true");
    sourceTask.start(props);

    when(sourceTask.getMockProcessor().process(any(), any()))
        .thenThrow(new RuntimeException("Test exception"));

    BytesXMLMessage smfMessage = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
    sourceTask.getIngressMessageQueue().add(smfMessage);

    assertThat(sourceTask.poll()).isNotNull().isEmpty();
    verify(smfMessage).ackMessage();
    assertThat(sourceTask.getMessageTracker()).satisfies(assertMessageTrackerEmpty());
  }

  // ========== stop() Tests ==========

  @Test
  void testStop_ClearsMessageTracker() throws Exception {
    sourceTask.start(createTestProps("test-topic"));

    SourceRecord record1 = createSourceRecord("topic1", "value1");
    SourceRecord record2 = createSourceRecord("topic2", "value2");
    when(sourceTask.getMockProcessor().getRecords(anyString()))
        .thenReturn(new SourceRecord[]{record1})
        .thenReturn(new SourceRecord[]{record2});

    BytesXMLMessage message1 = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    BytesXMLMessage message2 = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    sourceTask.getIngressMessageQueue().add(message1);
    sourceTask.getIngressMessageQueue().add(message2);

    sourceTask.poll();
    sourceTask.poll();

    MessageTracker tracker = sourceTask.getMessageTracker();
    assertThat(tracker.getRecordToMessageMap()).hasSize(2);
    assertThat(tracker.getMessagePendingRecords()).hasSize(2);

    sourceTask.stop();

    assertThat(tracker.getRecordToMessageMap()).isEmpty();
    assertThat(tracker.getMessagePendingRecords()).isEmpty();
  }

  @Test
  void testStop_UnpolledMessages_NotAcked() {
    sourceTask.start(createTestProps(null));

    BytesXMLMessage smfMessage1 = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
    BytesXMLMessage smfMessage2 = spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));

    BlockingQueue<BytesXMLMessage> ingressMessages = sourceTask.getIngressMessageQueue();
    ingressMessages.add(smfMessage1);
    ingressMessages.add(smfMessage2);

    assertThat(ingressMessages).hasSize(2);

    sourceTask.stop();

    verify(smfMessage1, never()).ackMessage();
    verify(smfMessage2, never()).ackMessage();
    assertThat(ingressMessages).isEmpty();
  }

  @Nested
  class MessageTrackerTests {

    private final MessageTracker tracker = sourceTask.getMessageTracker();

    @Test
    void testTrackSingleRecord() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord sourceRecord = createSourceRecord("test-topic", "test-value");

      tracker.track(message, new SourceRecord[]{sourceRecord});

      assertThat(tracker.getRecordToMessageMap())
          .hasSize(1)
          .containsEntry(sourceRecord, message);
      assertThat(tracker.getMessagePendingRecords()).hasSize(1);
      assertThat(tracker.getMessagePendingRecords().get(message))
          .isNotNull()
          .singleElement()
          .isEqualTo(sourceRecord);
    }

    @Test
    void testTrackMultipleRecords() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord record1 = createSourceRecord("topic1", "value1");
      SourceRecord record2 = createSourceRecord("topic2", "value2");
      SourceRecord record3 = createSourceRecord("topic3", "value3");

      tracker.track(message, new SourceRecord[]{record1, record2, record3});

      assertThat(tracker.getRecordToMessageMap())
          .hasSize(3)
          .containsEntry(record1, message)
          .containsEntry(record2, message)
          .containsEntry(record3, message);
      assertThat(tracker.getMessagePendingRecords()).hasSize(1);
      assertThat(tracker.getMessagePendingRecords().get(message))
          .isNotNull()
          .hasSize(3)
          .containsExactlyInAnyOrder(record1, record2, record3);
    }

    @Test
    void testCommitRecordSingleRecord_ReturnsMessage() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord sourceRecord = createSourceRecord("test-topic", "test-value");
      tracker.track(message, new SourceRecord[]{sourceRecord});

      assertThat(tracker.commitRecord(sourceRecord)).isSameAs(message);
      assertThat(tracker).satisfies(assertMessageTrackerEmpty());
    }

    @CartesianTest
    void testCommitRecordMultipleRecords_ReturnsNullUntilAllCommitted(
        @CartesianTest.Values(ints = {2, 3, 5}) int recordCount,
        @CartesianTest.Enum(CommitOrder.class) CommitOrder order) {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord[] records = IntStream.range(0, recordCount)
          .mapToObj(i -> createSourceRecord("topic", "value" + i))
          .toArray(SourceRecord[]::new);
      tracker.track(message, records);

      int[] commitOrder = order.generateCommitOrder(recordCount);

      for (int i = 0; i < recordCount - 1; i++) {
        assertThat(tracker.commitRecord(records[commitOrder[i]]))
            .as("Should return null when records are still pending (order: %s)", order)
            .isNull();
        assertThat(tracker.getMessagePendingRecords().get(message)).isNotNull()
            .hasSize(recordCount - i - 1);
      }

      assertThat(tracker.commitRecord(records[commitOrder[recordCount - 1]]))
          .as("Should return message when all records are committed (order: %s)", order)
          .isSameAs(message);
      assertThat(tracker).satisfies(assertMessageTrackerEmpty());
    }

    @Test
    void testCommitRecordUnknownRecord_ReturnsNull() {
      SourceRecord unknownRecord = createSourceRecord("unknown-topic", "unknown-value");

      assertThat(tracker.commitRecord(unknownRecord)).isNull();
      assertThat(tracker).satisfies(assertMessageTrackerEmpty());
    }

    @Test
    void testCommitRecordDuplicate_ReturnsNullAndLogsWarning() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord sourceRecord = createSourceRecord("test-topic", "test-value");
      tracker.track(message, new SourceRecord[]{sourceRecord});

      assertThat(tracker.commitRecord(sourceRecord)).isSameAs(message);
      assertThat(tracker.commitRecord(sourceRecord)).isNull();
    }

    @Test
    void testCommitRecordInconsistentState_NoPendingRecordsSet() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord sourceRecord = createSourceRecord("test-topic", "test-value");

      tracker.getRecordToMessageMap().put(sourceRecord, message);

      assertThat(tracker.commitRecord(sourceRecord)).isNull();
      assertThat(tracker.getRecordToMessageMap()).isEmpty();
    }

    @Test
    void testCommitRecordInconsistentState_RecordNotInPendingSet() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord sourceRecord = createSourceRecord("test-topic", "test-value");
      SourceRecord otherSourceRecord = createSourceRecord("other-topic", "other-value");

      tracker.getRecordToMessageMap().put(sourceRecord, message);
      tracker.getMessagePendingRecords().put(message, Collections.singleton(otherSourceRecord));

      assertThat(tracker.commitRecord(sourceRecord)).isNull();
      assertThat(tracker.getRecordToMessageMap()).isEmpty();
      assertThat(tracker.getMessagePendingRecords().get(message)).hasSize(1);
    }

    // ========== Clear Tests ==========

    @Test
    void testClearEmptyTracker() {
      tracker.clear();

      assertThat(tracker).satisfies(assertMessageTrackerEmpty());
    }

    @Test
    void testClearWithPendingMessages() {
      BytesXMLMessage message1 = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      BytesXMLMessage message2 = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord record1a = createSourceRecord("topic1", "value1a");
      SourceRecord record1b = createSourceRecord("topic1", "value1b");
      SourceRecord record2a = createSourceRecord("topic2", "value2a");
      SourceRecord record2b = createSourceRecord("topic2", "value2b");

      tracker.track(message1, new SourceRecord[]{record1a, record1b});
      tracker.track(message2, new SourceRecord[]{record2a, record2b});

      assertThat(tracker.getRecordToMessageMap()).hasSize(4);
      assertThat(tracker.getMessagePendingRecords()).hasSize(2);

      tracker.clear();

      assertThat(tracker).satisfies(assertMessageTrackerEmpty());
    }

    @Test
    void testClearAfterPartialCommit() {
      BytesXMLMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      SourceRecord record1 = createSourceRecord("topic", "value1");
      SourceRecord record2 = createSourceRecord("topic", "value2");
      SourceRecord record3 = createSourceRecord("topic", "value3");

      tracker.track(message, new SourceRecord[]{record1, record2, record3});

      assertThat(tracker.commitRecord(record1)).isNull();
      assertThat(tracker.commitRecord(record2)).isNull();

      assertThat(tracker.getRecordToMessageMap()).hasSize(1);
      assertThat(tracker.getMessagePendingRecords()).hasSize(1);
      assertThat(tracker.getMessagePendingRecords().get(message)).hasSize(1);

      tracker.clear();

      assertThat(tracker).satisfies(assertMessageTrackerEmpty());
    }
  }

  /**
   * Test subclass that overrides factory methods to inject mocks.
   */
  static class TestSolaceSourceTask extends SolaceSourceTask {
    private boolean useMockProcessor = true;
    private final SolMessageProcessorIF mockProcessor = mock(SolMessageProcessorIF.class);
    private final SolSessionHandler mockSessionHandler = mock(SolSessionHandler.class);
    private final SolaceSourceTopicListener mockTopicListener = mock(SolaceSourceTopicListener.class);
    private final SolaceSourceQueueConsumer mockQueueConsumer = mock(SolaceSourceQueueConsumer.class);

    SolMessageProcessorIF getMockProcessor() {
      return mockProcessor;
    }

    SolSessionHandler getMockSessionHandler() {
      return mockSessionHandler;
    }

    SolaceSourceTopicListener getMockTopicListener() {
      return mockTopicListener;
    }

    SolaceSourceQueueConsumer getMockQueueConsumer() {
      return mockQueueConsumer;
    }

    void doNotUseMockProcessor() {
      useMockProcessor = false;
    }

    @Override
    SolaceSourceConnectorConfig createConnectorConfig(Map<String, String> props) {
      SolaceSourceConnectorConfig config = spy(super.createConnectorConfig(props));

      if (useMockProcessor) {
        doReturn(mockProcessor)
            .when(config)
            .getConfiguredInstance(
                SolaceSourceConstants.SOL_MESSAGE_PROCESSOR,
                SolMessageProcessorIF.class);
      }

      return config;
    }

    @Override
    SolSessionHandler createSessionHandler(SolaceSourceConnectorConfig config) {
      return mockSessionHandler;
    }

    @Override
    SolaceSourceTopicListener createTopicListener(SolaceSourceConnectorConfig config,
        SolSessionHandler handler) {
      return mockTopicListener;
    }

    @Override
    SolaceSourceQueueConsumer createQueueConsumer(SolaceSourceConnectorConfig config,
        SolSessionHandler handler) {
      return mockQueueConsumer;
    }
  }

  /**
   * Enum for testing different commit order strategies.
   */
  private enum CommitOrder {
    SEQUENTIAL,  // Commit in order: 0, 1, 2, ...
    REVERSE,     // Commit in reverse: N-1, N-2, ..., 0
    RANDOM;      // Commit in pseudo-random order

    /**
     * Helper method to generate commit order indices based on the specified strategy.
     */
    int[] generateCommitOrder(int recordCount) {
      int[] commitOrder = new int[recordCount];
      switch (this) {
        case SEQUENTIAL:
          for (int i = 0; i < recordCount; i++) {
            commitOrder[i] = i;
          }
          break;
        case REVERSE:
          for (int i = 0; i < recordCount; i++) {
            commitOrder[i] = recordCount - 1 - i;
          }
          break;
        case RANDOM:
          // Pseudo-random but deterministic: interleave odd/even indices
          int idx = 0;
          for (int i = 0; i < recordCount; i += 2) {
            commitOrder[idx++] = i;  // 0, 2, 4, ...
          }
          for (int i = 1; i < recordCount; i += 2) {
            commitOrder[idx++] = i;  // 1, 3, 5, ...
          }
          break;
        default:
           throw new IllegalStateException("Unexpected commit order: " + this);
      }
      return commitOrder;
    }
  }

  /**
   * Enum for testing different "no records" scenarios in poll() tests.
   */
  private enum RecordsScenario {
    NULL_RECORDS(null),
    EMPTY_ARRAY(new SourceRecord[0]);

    private final SourceRecord[] records;

    RecordsScenario(SourceRecord[] records) {
      this.records = records;
    }

    SourceRecord[] getRecords() {
      return records;
    }
  }

  // ========== Helper Methods ==========

  /**
   * Asserts that a MessageTracker has no pending messages (both maps are empty).
   */
  private static Consumer<MessageTracker> assertMessageTrackerEmpty() {
    return tracker -> {
      assertThat(tracker.getRecordToMessageMap()).isEmpty();
      assertThat(tracker.getMessagePendingRecords()).isEmpty();
    };
  }

  /**
   * Helper method to create test properties with minimal required config.
   */
  private Map<String, String> createTestProps(String kafkaTopic) {
    Map<String, String> props = new HashMap<>();
    if (kafkaTopic != null) {
      props.put(SolaceSourceConstants.KAFKA_TOPIC, kafkaTopic);
    }
    return props;
  }

  /**
   * Helper method to create a real SourceRecord instance for testing.
   */
  private SourceRecord createSourceRecord(String topic, String value) {
    return new SourceRecord(
        null,    // sourcePartition
        null,                  // sourceOffset
        topic,                 // topic
        null,                  // partition
        Schema.STRING_SCHEMA,  // keySchema
        null,                  // key
        Schema.STRING_SCHEMA,  // valueSchema
        value,                 // value
        null                   // timestamp
    );
  }
}
