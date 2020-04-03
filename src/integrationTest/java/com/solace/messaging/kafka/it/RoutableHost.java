package com.solace.messaging.kafka.it;

import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RoutableHost {
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
    return (String) HostAddresses.toArray()[0];  }
}
