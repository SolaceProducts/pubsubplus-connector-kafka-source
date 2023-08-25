package com.solace.connector.kafka.connect.source.it;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.solace.connector.kafka.connect.source.SolaceSourceConnector;
import com.solace.connector.kafka.connect.source.VersionUtil;
import com.solace.connector.kafka.connect.source.it.util.KafkaConnection;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SolaceConnectorDeployment implements TestConstants {

  static Logger logger = LoggerFactory.getLogger(SolaceConnectorDeployment.class);

  private final OkHttpClient client = new OkHttpClient();
  private final KafkaConnection kafkaConnection;
  private final String kafkaTopic;

  public SolaceConnectorDeployment(KafkaConnection kafkaConnection, String kafkaTopic) {
    this.kafkaConnection = kafkaConnection;
    this.kafkaTopic = kafkaTopic;
  }

  public void waitForConnectorRestIFUp() {
    Request request = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connector-plugins").build();
    assertTimeoutPreemptively(Duration.ofMinutes(15), () -> {
      Response response = null;
      do {
        try {
          Thread.sleep(1000L);
          response = client.newCall(request).execute();
        } catch (IOException | InterruptedException e) {
          logger.error("Failed to get connector-plugins", e);
        }
      } while (response == null || !response.isSuccessful());
    });
  }

  void startConnector(Properties props) {
    startConnector(props, false);
  }

  void startConnector(Properties props, boolean expectStartFail) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String configJson = null;
    // Prep config files
    try {
      // Configure .json connector params
      File jsonFile = new File(
          UNZIPPEDCONNECTORDESTINATION + "/" + Tools.getUnzippedConnectorDirName() + "/" + CONNECTORJSONPROPERTIESFILE);
      String jsonString = FileUtils.readFileToString(jsonFile);
      JsonElement jtree = new JsonParser().parse(jsonString);
      JsonElement jconfig = jtree.getAsJsonObject().get("config");
      JsonObject jobject = jconfig.getAsJsonObject();
      // Set properties defaults
      jobject.addProperty("kafka.topic", kafkaTopic);
      jobject.addProperty("sol.topics", SOL_TOPICS);
      jobject.addProperty("sol.queue", SOL_QUEUE);
      jobject.addProperty("sol.message_processor_class", CONN_MSGPROC_CLASS);
      jobject.addProperty("sol.kafka_message_key", CONN_KAFKA_MSGKEY);
      jobject.addProperty("value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
      jobject.addProperty("key.converter", "org.apache.kafka.connect.storage.StringConverter");
      jobject.addProperty("tasks.max", "1");
      // Override properties if provided
      props.forEach((key, value) -> jobject.addProperty((String) key, (String) value));
      configJson = gson.toJson(jtree);
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Configure and start the solace connector
    try {
      // check presence of Solace plugin: curl
      // http://18.218.82.209:8083/connector-plugins | jq
      Request request = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connector-plugins").build();
      try (Response response = client.newCall(request).execute()) {
        assertTrue(response.isSuccessful());
        JsonArray results = responseBodyToJson(response.body()).getAsJsonArray();
        logger.info("Available connector plugins: " + gson.toJson(results));
        boolean hasConnector = false;
        for (Iterator<JsonElement> resultsIter = results.iterator(); !hasConnector && resultsIter.hasNext();) {
          JsonObject connectorPlugin = resultsIter.next().getAsJsonObject();
          if (connectorPlugin.get("class").getAsString().equals(SolaceSourceConnector.class.getName())) {
            hasConnector = true;
            assertEquals("source", connectorPlugin.get("type").getAsString());
            assertEquals(VersionUtil.getVersion(), connectorPlugin.get("version").getAsString());
          }
        }
        assertTrue(hasConnector, String.format("Could not find connector %s : %s",
                SolaceSourceConnector.class.getName(), gson.toJson(results)));
      }

      // Delete a running connector, if any
      deleteConnector();

      // configure plugin: curl -X POST -H "Content-Type: application/json" -d
      // @solace_source_properties.json http://18.218.82.209:8083/connectors
      Request configrequest = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connectors")
          .post(RequestBody.create(configJson, MediaType.parse("application/json"))).build();
      try (ResponseBody configresponse = client.newCall(configrequest).execute().body()) {
        assertNotNull(configresponse);
        String configresults = configresponse.string();
        logger.info("Connector config results: " + configresults);
      }

      // check success
      AtomicReference<JsonObject> statusResponse = new AtomicReference<>(new JsonObject());
      assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
        JsonObject connectorStatus;
        do {
          connectorStatus = getConnectorStatus();
          statusResponse.set(connectorStatus);
        } while (!(expectStartFail ? "FAILED" : "RUNNING").equals(Optional.ofNullable(connectorStatus)
                .map(a -> a.getAsJsonArray("tasks"))
                .map(a -> a.size() > 0 ? a.get(0) : null)
                .map(JsonElement::getAsJsonObject)
                .map(a -> a.get("state"))
                .map(JsonElement::getAsString)
                .orElse("")));
      }, () -> "Timed out while waiting for connector to start: " + gson.toJson(statusResponse.get()));
      Thread.sleep(10000); // Give some extra time to start
      logger.info("Connector is now RUNNING");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void deleteConnector() throws IOException {
    Request request = new Request.Builder().url(kafkaConnection.getConnectUrl() + "/connectors/solaceSourceConnector")
            .delete().build();
    try (Response response = client.newCall(request).execute()) {
      logger.info("Delete response: " + response);
    }
  }

  public JsonObject getConnectorStatus() {
    Request request = new Request.Builder()
            .url(kafkaConnection.getConnectUrl() + "/connectors/solaceSourceConnector/status").build();
    return assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
      while (true) {
        try (Response response = client.newCall(request).execute()) {
          if (!response.isSuccessful()) {
            continue;
          }

          return responseBodyToJson(response.body()).getAsJsonObject();
        }
      }
    });
  }

  private JsonElement responseBodyToJson(ResponseBody responseBody) {
    return Optional.ofNullable(responseBody)
            .map(ResponseBody::charStream)
            .map(s -> new JsonParser().parse(s))
            .orElseGet(JsonObject::new);
  }
}
