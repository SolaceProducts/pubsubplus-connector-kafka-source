package com.solace.messaging.kafka.it;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.impl.AbstractDestination;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SourceConnectorIT implements TestConstants {

    static Logger logger = LoggerFactory.getLogger(SourceConnectorIT.class.getName());
    // Connectordeployment creates a Kafka topic "kafkaTestTopic", which is used next
    static SolaceConnectorDeployment connectorDeployment = new SolaceConnectorDeployment();
    static TestKafkaConsumer kafkaConsumer = new TestKafkaConsumer(SolaceConnectorDeployment.kafkaTestTopic);
    static TestSolaceProducer solaceProducer = new TestSolaceProducer();
    
    ////////////////////////////////////////////////////
    // Main setup/teardown

    @BeforeAll
    static void setUp() {
        // Start consumer
        kafkaConsumer.run();
        try {
            Thread.sleep(1000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    static void cleanUp() {
        kafkaConsumer.stop();
        solaceProducer.close();
    }
    
    ////////////////////////////////////////////////////
    // Test types
    
    void messageToKafkaTest(Message msg, AbstractDestination destination, String expectedValue, Object expectedKey) {
        try {
            // Clean catch queue first
            // TODO: fix possible concurrency issue with cleaning/wring the queue later
            TestKafkaConsumer.kafkaReceivedMessages.clear();
            // Send Solace message
            if (destination instanceof Topic) {
                solaceProducer.sendMessageToTopic((Topic) destination, msg);
            } else {
                solaceProducer.sendMessageToQueue((Queue) destination, msg);
            }
            // Wait for Kafka to report message
            ConsumerRecord<Object, Object> record = TestKafkaConsumer.kafkaReceivedMessages.poll(5,TimeUnit.SECONDS);
            // Evaluate message
            assert(record != null);
            logger.info("Kafka message received - Key=" + record.key() + ", Value=" + record.value());
            assert record.value().equals(expectedValue);
            // Check key
            if (expectedKey == null) {
                assert(record.key() == null);
            } else {
                assert (record.key() instanceof ByteBuffer);
                ByteBuffer bb = (ByteBuffer) record.key();
                byte[] b = new byte[bb.remaining()];
                bb.get(b);
                if (expectedKey instanceof String) {
                    assert(Arrays.equals( b, ((String) expectedKey).getBytes()));
                } else {
                    assert(Arrays.equals( b, (byte[]) expectedKey));
                }
            }
        } catch (JCSMPException e1) {
             e1.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    ////////////////////////////////////////////////////
    // Scenarios
    
    @DisplayName("Solace connector SimpleMessageProcessor tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorSimpleMessageProcessorTests {
        
        ////////////////////////////////////////////////////
        // Scenarios
        
        @BeforeAll
        void setUp() {
            solaceProducer.resetQueue(SOL_QUEUE);
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolSampleSimpleMessageProcessor");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic");
            prop.setProperty("sol.username", "test");
            prop.setProperty("sol.password", "test");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("1-Hello TextMessageToTopicTest world!");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "1-Hello TextMessageToTopicTest world!", null);
        }
        
        @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                    new byte[] {'2','-','H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "2-Hello Topic world!", null);
        }

        
        @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(null);
            msg.writeAttachment(new byte[] {'3','-','H','e','l','l','o',' ','a','t','t','a','c','h','e','d',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "3-Hello attached world!", null);
        }

        
        @DisplayName("TextMessage-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextmessageToKafkaTest() {
            TextMessage msg = solaceProducer.createTextMessage("4-Hello TextmessageToKafkaTest world!");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "4-Hello TextmessageToKafkaTest world!", null);
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerBytesmessageToKafkaTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                new byte[] {'5','-','H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "5-Hello Queue world!", null);
        }

        @DisplayName("ByteMessage-AttachmentPayload-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageWithAttachmentPayloadToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(null);
            msg.writeAttachment(new byte[] {'6','-','H','e','l','l','o',' ','a','t','t','a','c','h','e','d',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "6-Hello attached world!", null);
        }

    }
    
    //////////////////////////////////////////////////////////////////////////////////////////

    @DisplayName("Solace connector SolSampleKeyedMessageProcessor-NONE tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorNoneKeyedMessageProcessorTests {
        
        @BeforeAll
        void setUp() {
            solaceProducer.resetQueue(SOL_QUEUE);
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolaceSampleKeyedMessageProcessor");
            prop.setProperty("sol.kafka_message_key", "NONE");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", null);
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
                    messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                                    // expected value & key:
                                    "Hello Topic world!", null);
        }
        
        @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(null);
            msg.writeAttachment(new byte[] {'H','e','l','l','o',' ','a','t','t','a','c','h','e','d',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello attached world!", null);
        }

        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextmessageToKafkaTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello TextmessageToKafkaTest world!", null);
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesmessageToKafkaTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
                        messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                                        // expected value & key:
                                        "Hello Queue world!", null);
        }

        @DisplayName("ByteMessage-AttachmentPayload-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageWithAttachmentPayloadToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(null);
            msg.writeAttachment(new byte[] {'H','e','l','l','o',' ','a','t','t','a','c','h','e','d',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello attached world!", null);
        }

    }
    
    //////////////////////////////////////////////////////////////////////////////////////////

    @DisplayName("Solace connector SolSampleKeyedMessageProcessor-DESTINATION tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorDestinationKeyedMessageProcessorTests {
        
        @BeforeAll
        void setUp() {
            solaceProducer.resetQueue(SOL_QUEUE);
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolaceSampleKeyedMessageProcessor");
            prop.setProperty("sol.kafka_message_key", "DESTINATION");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", "TestTopic1/SubTopic");
       }
        
        @DisplayName("TextMessage-Topic-wildcard-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest2() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest2 world!");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic2/SubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest2 world!", "TestTopic2/SubTopic");
       }
        
        @DisplayName("TextMessage-Topic-multi-level-wildcard-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest3() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest3 world!");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic3/SubTopic/SubSubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest3 world!", "TestTopic3/SubTopic/SubSubTopic");
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
                    messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                                    // expected value & key:
                                    "Hello Topic world!", "TestTopic1/SubTopic");
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextmessageToKafkaTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello TextmessageToKafkaTest world!", SOL_QUEUE);
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesmessageToKafkaTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
                        messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                                        // expected value & key:
                                        "Hello Queue world!", SOL_QUEUE);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////
    
    @DisplayName("Solace connector SolSampleKeyedMessageProcessor-CORRELATION_ID tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorCorrelationIdKeyedMessageProcessorTests {
        
        @BeforeAll
        void setUp() {
            solaceProducer.resetQueue(SOL_QUEUE);
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolaceSampleKeyedMessageProcessor");
            prop.setProperty("sol.kafka_message_key", "CORRELATION_ID");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
            msg.setCorrelationId("test");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", "test");
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test2");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello Topic world!", "test2");
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextmessageToKafkaTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
            msg.setCorrelationId("test3");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello TextmessageToKafkaTest world!", "test3");
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesmessageToKafkaTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test4");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello Queue world!", "test4");
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////
    
    @DisplayName("Solace connector SolSampleKeyedMessageProcessor-CORRELATION_ID_AS_BYTES tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorCorrelationIdAsBytesKeyedMessageProcessorTests {
        
        @BeforeAll
        void setUp() {
            solaceProducer.resetQueue(SOL_QUEUE);
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolaceSampleKeyedMessageProcessor");
            prop.setProperty("sol.kafka_message_key", "CORRELATION_ID_AS_BYTES");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
            prop.setProperty("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
            msg.setCorrelationId(new String(new byte[] { 1,2,3,4 }));
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", new String(new byte[] { 1,2,3,4 }) );
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test2");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello Topic world!", "test2");
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextmessageToKafkaTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextmessageToKafkaTest world!");
            msg.setCorrelationId("test3");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello TextmessageToKafkaTest world!", "test3");
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesmessageToKafkaTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test4");
            messageToKafkaTest(msg, solaceProducer.defineQueue(SOL_QUEUE),
                            // expected value & key:
                            "Hello Queue world!", "test4");
        }
    }

    ////////////////////////////////////////////////////
    // Scenarios
    
    @DisplayName("Solace connector SharedSubscriptions tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorSharedSubscriptionsTests {
        
        @BeforeAll
        void setUp() {
            solaceProducer.resetQueue(SOL_QUEUE);
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolSampleSimpleMessageProcessor");
            prop.setProperty("sol.topics", "#share/group1/TestTopic1/SubTopic");
            prop.setProperty("tasks.max", "5");
            connectorDeployment.startConnector(prop);
        }
    
    
        @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest world!");
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello TextMessageToTopicTest world!", null);
        }
        
        @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                    new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello Topic world!", null);
        }
    
        
        @DisplayName("ByteMessage-AttachmentPayload-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageWithAttachmentPayloadToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(null);
            msg.writeAttachment(new byte[] {'H','e','l','l','o',' ','a','t','t','a','c','h','e','d',' ','w','o','r','l','d','!'});
            messageToKafkaTest(msg, solaceProducer.defineTopic("TestTopic1/SubTopic"),
                            // expected value & key:
                            "Hello attached world!", null);
        }
    
    }
}
