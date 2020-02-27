package com.solace.messaging;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.TextMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class ConnectorIT implements TestConstants {

    static Logger logger = LoggerFactory.getLogger(ConnectorIT.class.getName());
    static TestKafkaConsumer kafkaConsumer = new TestKafkaConsumer();
    static TestSolaceProducer solaceProducer = new TestSolaceProducer("tcp://" + MessagingServiceFullLocalSetup.COMPOSE_CONTAINER_PUBSUBPLUS
                            .getServiceHost("solbroker_1", 55555) + ":55555", "default", "default", "default");
    static SolaceConnectorDeployment connectorDeployment = new SolaceConnectorDeployment();
    
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

        // Ensure test queue exists on PubSub+
        try {
            solaceProducer.provisionQueue(SOL_QUEUE);
        } catch (JCSMPException e1) {
            e1.printStackTrace();
        }
    }

    @AfterAll
    static void cleanUp() {
        kafkaConsumer.stop();
        solaceProducer.close();
    }

    
    ////////////////////////////////////////////////////
    // Test types
    
    void messageToTopicTest(Message msg, String topic, String expectedValue, String expectedKey) {
        try {
            solaceProducer.sendMessageToTopic(topic, msg);
            ConsumerRecord<String, String> record = TestKafkaConsumer.kafkaReceivedMessages.poll(500,TimeUnit.SECONDS);
            assert(record != null);
            assert record.value().contentEquals(expectedValue);
            assert(expectedKey == null ? record.key() == null : record.key().contentEquals(expectedKey));
        } catch (JCSMPException e1) {
             e1.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    void messageToQueueTest(Message msg, String queue, String expectedValue, String expectedKey) {
        try {
            solaceProducer.sendMessageToQueue(queue, msg);
            ConsumerRecord<String, String> record = TestKafkaConsumer.kafkaReceivedMessages.poll(5,TimeUnit.SECONDS);
            assert(record != null);
            assert record.value().contentEquals(expectedValue);
            assert(expectedKey == null ? record.key() == null : record.key().contentEquals(expectedKey));
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
        
        @BeforeAll
        void setUp() {
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolSampleSimpleMessageProcessor");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest world!");
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest world!", null);
        }
        
        @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                    new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello Topic world!", null);
        }

        
        // TODO: Binary attachment pay load
        // messageOut = msg.getAttachmentByteBuffer().array()
        
        
        @DisplayName("TextMessage-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToQueueTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToQueueTest world!");
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello TextMessageToQueueTest world!", null);
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerBytesMessageToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello Queue world!", null);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////

    @DisplayName("Solace connector SolSampleKeyedMessageProcessor-NONE tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorNoneKeyedMessageProcessorTests {
        
        @BeforeAll
        void setUp() {
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
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", null);
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
                    messageToTopicTest(msg, "TestTopic1/SubTopic",
                                    // expected value & key:
                                    "Hello Topic world!", null);
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToQueueTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToQueueTest world!");
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello TextMessageToQueueTest world!", null);
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesMessageToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
                        messageToQueueTest(msg, SOL_QUEUE,
                                        // expected value & key:
                                        "Hello Queue world!", null);
        }
    }
    
    //////////////////////////////////////////////////////////////////////////////////////////

    @DisplayName("Solace connector SolSampleKeyedMessageProcessor-DESTINATION tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorDestinationKeyedMessageProcessorTests {
        
        @BeforeAll
        void setUp() {
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
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", "TestTopic1/SubTopic");
       }
        
        @DisplayName("TextMessage-Topic-wildcard-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest2() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest2 world!");
            messageToTopicTest(msg, "TestTopic2/SubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest2 world!", "TestTopic2/SubTopic");
       }
        
        @DisplayName("TextMessage-Topic-multi-level-wildcard-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest3() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest3 world!");
            messageToTopicTest(msg, "TestTopic3/SubTopic/SubSubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest3 world!", "TestTopic3/SubTopic/SubSubTopic");
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
                    messageToTopicTest(msg, "TestTopic1/SubTopic",
                                    // expected value & key:
                                    "Hello Topic world!", "TestTopic1/SubTopic");
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToQueueTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToQueueTest world!");
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello TextMessageToQueueTest world!", SOL_QUEUE);
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesMessageToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
                        messageToQueueTest(msg, SOL_QUEUE,
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
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", "test");
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test2");
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello Topic world!", "test2");
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToQueueTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToQueueTest world!");
            msg.setCorrelationId("test3");
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello TextMessageToQueueTest world!", "test3");
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesMessageToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test4");
            messageToQueueTest(msg, SOL_QUEUE,
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
            Properties prop = new Properties();
            prop.setProperty("sol.message_processor_class", "com.solace.source.connector.msgprocessors.SolaceSampleKeyedMessageProcessor");
            prop.setProperty("sol.kafka_message_key", "CORRELATION_ID_AS_BYTES");
            prop.setProperty("sol.topics", "TestTopic1/SubTopic,TestTopic2/*,TestTopic3/>");
            connectorDeployment.startConnector(prop);
        }


        @DisplayName("TextMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToTopicTest1 world!");
            msg.setCorrelationId(new String(new char[] { 1,2,3,4 }));
            String expectedKey = new SchemaAndValue(Schema.OPTIONAL_BYTES_SCHEMA,
                            new byte[] { 1,2,3,4 }).value().toString();
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello TextMessageToTopicTest1 world!", expectedKey );
       }
        
        @DisplayName("ByteMessage-Topic-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test2");
            messageToTopicTest(msg, "TestTopic1/SubTopic",
                            // expected value & key:
                            "Hello Topic world!", "test2");
        }
        
        @DisplayName("TextMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToQueueTest() {
            TextMessage msg = solaceProducer.createTextMessage("Hello TextMessageToQueueTest world!");
            msg.setCorrelationId("test3");
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello TextMessageToQueueTest world!", "test3");
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleKeyedMessageProcessor")
        @Test
        void kafkaConsumerBytesMessageToQueueTest() {
            BytesMessage msg = solaceProducer.createBytesMessage(
                            new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
            msg.setCorrelationId("test4");
            messageToQueueTest(msg, SOL_QUEUE,
                            // expected value & key:
                            "Hello Queue world!", "test4");
        }
    }
}
