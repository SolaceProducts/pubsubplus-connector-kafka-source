package com.solace.messaging;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    
    @DisplayName("Solace connector tests")
    @Nested
    @TestInstance(Lifecycle.PER_CLASS)
    class SolaceConnectorTests {
        
        @BeforeAll
        void startConnector() {
            // Prep config files
            try {
                // Configure .config connector params
                Parameters params = new Parameters();
                FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(
                                PropertiesConfiguration.class)
                                                .configure(params.properties().setFileName(CONNECTORPROPERTIESFILE));
                Configuration config = builder.getConfiguration();
                config.setProperty("sol.host", "tcp://" + MessagingServiceFullLocalSetup.COMPOSE_CONTAINER_PUBSUBPLUS
                                .getServiceHost("solbroker_1", 55555) + ":55555");
                config.setProperty("sol.username", SOL_ADMINUSER_NAME);
                config.setProperty("sol.password", SOL_ADMINUSER_PW);
                config.setProperty("sol.vpn_name", SOL_VPN);
                config.setProperty("kafka.topic", KAFKA_TOPIC);
                config.setProperty("sol.topics", SOL_TOPICS);
                config.setProperty("sol.queue", SOL_QUEUE);
                config.setProperty("sol.message_processor_class", CONN_MSGPROC_CLASS);
                config.setProperty("sol.kafka_message_key", CONN_KAFKA_MSGKEY);
                builder.save();

                // Configure .json connector params
                File jsonFile = new File(CONNECTORJSONPROPERTIESFILE);
                String jsonString = FileUtils.readFileToString(jsonFile);
                JsonElement jtree = new JsonParser().parse(jsonString);
                JsonElement jconfig = jtree.getAsJsonObject().get("config");
                JsonObject jobject = jconfig.getAsJsonObject();
                jobject.addProperty("sol.host", "tcp://" + MessagingServiceFullLocalSetup.COMPOSE_CONTAINER_PUBSUBPLUS
                                .getServiceHost("solbroker_1", 55555) + ":55555");
                jobject.addProperty("sol.username", SOL_ADMINUSER_NAME);
                jobject.addProperty("sol.password", SOL_ADMINUSER_PW);
                jobject.addProperty("sol.vpn_name", SOL_VPN);
                jobject.addProperty("kafka.topic", KAFKA_TOPIC);
                jobject.addProperty("sol.topics", SOL_TOPICS);
                jobject.addProperty("sol.queue", SOL_QUEUE);
                jobject.addProperty("sol.message_processor_class", CONN_MSGPROC_CLASS);
                jobject.addProperty("sol.kafka_message_key", CONN_KAFKA_MSGKEY);
                Gson gson = new Gson();
                String resultingJson = gson.toJson(jtree);
                FileUtils.writeStringToFile(jsonFile, resultingJson);
            } catch (ConfigurationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Configure and start the solace connector
            try {
                OkHttpClient client = new OkHttpClient();
                // TODO: Fix to correct way to determine host! 
                String connectorAddress = MessagingServiceFullLocalSetup.COMPOSE_CONTAINER_KAFKA
                                    .getServiceHost("kafka_1", 9092) + ":28083";
                // check presence of Solace plugin: curl
                // http://18.218.82.209:8083/connector-plugins | jq
                Request request = new Request.Builder().url("http://" + connectorAddress + "/connector-plugins").build();
                Response response;
                    response = client.newCall(request).execute();
                assert (response.isSuccessful());
                String results = response.body().string();
                logger.info("Availabe connector plugins: " + results);
                assert (results.contains("solace"));
    
                // Delete a running connector, if any
                Request deleterequest = new Request.Builder().url("http://" + connectorAddress + "/connectors/solaceConnector")
                                .delete()
                                .build();
                Response deleteresponse = client.newCall(deleterequest).execute();
                logger.info("Delete response: " + deleteresponse);
                assert (deleteresponse.isSuccessful());
                
                // configure plugin: curl -X POST -H "Content-Type: application/json" -d
                // @solace_source_properties.json http://18.218.82.209:8083/connectors
                Request configrequest = new Request.Builder().url("http://" + connectorAddress + "/connectors")
                                .post(RequestBody.create(
                                                new String(Files.readAllBytes(Paths.get(CONNECTORJSONPROPERTIESFILE))),
                                                MediaType.parse("application/json")))
                                .build();
                Response configresponse = client.newCall(configrequest).execute();
                // if (!configresponse.isSuccessful()) throw new IOException("Unexpected code "
                // + configresponse);
                String configresults = configresponse.body().string();
                logger.info("Connector config results: " + configresults);
                // check success
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        @DisplayName("TextMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToTopicTest() {
            try {
                TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                msg.setText("Hello TextMessageToTopicTest world!");
                solaceProducer.sendMessageToTopic(SOL_TOPICS, msg);
                ConsumerRecord<String, String> record = TestKafkaConsumer.kafkaReceivedMessages.take();
                assert record.value().contentEquals("Hello TextMessageToTopicTest world!");
                Thread.sleep(1000l);
            } catch (JCSMPException e1) {
                 e1.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        @DisplayName("ByteMessage-Topic-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerByteMessageToTopicTest() {
            try {
                BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                msg.setData(new byte[] {'H','e','l','l','o',' ','T','o','p','i','c',' ','w','o','r','l','d','!'});
                solaceProducer.sendMessageToTopic(SOL_TOPICS, msg);
                ConsumerRecord<String, String> record = TestKafkaConsumer.kafkaReceivedMessages.take();
                assert record.value().contentEquals("Hello Topic world!");
                Thread.sleep(1000l);
            } catch (JCSMPException e1) {
                 e1.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        @DisplayName("TextMessage-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerTextMessageToQueueTest() {
            try {
                TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
                msg.setText("Hello TextMessageToQueueTest world!");
                solaceProducer.sendMessageToQueue(SOL_QUEUE, msg);
                ConsumerRecord<String, String> record = TestKafkaConsumer.kafkaReceivedMessages.poll(2,TimeUnit.SECONDS);
                assert(record != null);
                assert record.value().contentEquals("Hello TextMessageToQueueTest world!");
                Thread.sleep(1000l);
            } catch (JCSMPException e1) {
                 e1.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        @DisplayName("BytesMessage-Queue-SolSampleSimpleMessageProcessor")
        @Test
        void kafkaConsumerBytesMessageToQueueTest() {
            try {
                BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
                msg.setData(new byte[] {'H','e','l','l','o',' ','Q','u','e','u','e',' ','w','o','r','l','d','!'});
                solaceProducer.sendMessageToQueue(SOL_QUEUE, msg);
                ConsumerRecord<String, String> record = TestKafkaConsumer.kafkaReceivedMessages.poll(2,TimeUnit.SECONDS);
                assert(record != null);
                assert record.value().contentEquals("Hello Queue world!");
                Thread.sleep(1000l);
            } catch (JCSMPException e1) {
                 e1.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
    }
}
