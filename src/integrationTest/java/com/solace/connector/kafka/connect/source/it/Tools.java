package com.solace.connector.kafka.connect.source.it;

import com.solace.connector.kafka.connect.source.VersionUtil;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Tools {

  static public String getUnzippedConnectorDirName() {
    String connectorUnzippedPath = null;
    try {
      DirectoryStream<Path> dirs = Files.newDirectoryStream(
              Paths.get(TestConstants.UNZIPPEDCONNECTORDESTINATION),
              "pubsubplus-connector-kafka-source-" + VersionUtil.getVersion());
      for (Path entry: dirs) {
        connectorUnzippedPath = entry.toString();
        break; //expecting only one
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (connectorUnzippedPath.contains("\\")) {
      return connectorUnzippedPath.substring(connectorUnzippedPath.lastIndexOf("\\") + 1);
    }
    return connectorUnzippedPath.substring(connectorUnzippedPath.lastIndexOf("/") + 1);
  }
}
