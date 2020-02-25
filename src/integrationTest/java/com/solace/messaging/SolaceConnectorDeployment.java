package com.solace.messaging;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class SolaceConnectorDeployment  implements TestConstants {

    static Logger logger = LoggerFactory.getLogger(SolaceConnectorDeployment.class.getName());

    void startConnector() {
        startConnector(null); // Defaults only, no override
    }

    void startConnector(Properties props) {
        // Prep config files
        try {
            // Configure .config connector params
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder = new FileBasedConfigurationBuilder<FileBasedConfiguration>(
                            PropertiesConfiguration.class)
                                            .configure(params.properties().setFileName(CONNECTORPROPERTIESFILE));
            Configuration config = builder.getConfiguration();
            // Set properties defaults
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
            // Override properties if provided
            if (props != null) {
                props.forEach((key, value) -> {
                    config.setProperty((String) key, value);
                    logger.info("Overriding property " + key + " to " + value);
                });
            }
            builder.save();

            // Configure .json connector params
            File jsonFile = new File(CONNECTORJSONPROPERTIESFILE);
            String jsonString = FileUtils.readFileToString(jsonFile);
            JsonElement jtree = new JsonParser().parse(jsonString);
            JsonElement jconfig = jtree.getAsJsonObject().get("config");
            JsonObject jobject = jconfig.getAsJsonObject();
            // Set properties defaults
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
            // Override properties if provided
            if (props != null) {
                props.forEach((key, value) -> {
                    jobject.addProperty((String) key, (String) value);
                });
            }
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
            logger.info("Available connector plugins: " + results);
            assert (results.contains("solace"));

            // Delete a running connector, if any
            Request deleterequest = new Request.Builder().url("http://" + connectorAddress + "/connectors/solaceConnector")
                            .delete()
                            .build();
            Response deleteresponse = client.newCall(deleterequest).execute();
            logger.info("Delete response: " + deleteresponse);
            
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
            Thread.sleep(5000); // Give some time to start
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
