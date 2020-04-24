package com.solace.connector.kafka.connect.source.it;

import java.io.IOException;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class Tools {
  static public String getIpAddress() {
    Set<String> HostAddresses = new HashSet<>();
    try {
      for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
        if (!ni.isLoopback() && ni.isUp() && ni.getHardwareAddress() != null) {
          for (InterfaceAddress ia : ni.getInterfaceAddresses()) {
            if (ia.getBroadcast() != null) {  //If limited to IPV4
              HostAddresses.add(ia.getAddress().getHostAddress());
            }
          }
        }
      }
    } catch (SocketException e) { }
    return (String) HostAddresses.toArray()[0];
  }

  static public String getUnzippedConnectorDirName() {
    String connectorUnzippedPath = null;
    try {
      DirectoryStream<Path> dirs = Files.newDirectoryStream(
          Paths.get(TestConstants.UNZIPPEDCONNECTORDESTINATION), "pubsubplus-connector-kafka-*");
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
