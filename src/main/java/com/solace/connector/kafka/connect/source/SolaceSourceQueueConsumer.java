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
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.Queue;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSourceQueueConsumer {
  private static final Logger log = LoggerFactory.getLogger(SolaceSourceQueueConsumer.class);
  private SolaceSourceConnectorConfig lconfig;
  private Queue solQueue;
  private FlowReceiver recv;
  private SolMessageQueueCallbackHandler callbackhandler;
  private SolSessionHandler solSessionHandler;

  SolaceSourceQueueConsumer(SolaceSourceConnectorConfig lconfig, SolSessionHandler solSessionHandler) {
    this.lconfig = lconfig;
    this.solSessionHandler = solSessionHandler;
  }

  public boolean init(BlockingQueue<BytesXMLMessage> squeue) {
    solQueue = JCSMPFactory.onlyInstance().createQueue(lconfig.getString(SolaceSourceConstants.SOL_QUEUE));
    final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
    flow_prop.setEndpoint(solQueue);
    flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT); // Will explicitly ack at commit
    flow_prop.setStartState(true);
    EndpointProperties endpointProps = new EndpointProperties();
    endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
    try {
      callbackhandler = new SolMessageQueueCallbackHandler(squeue);
      recv = solSessionHandler.getSession().createFlow(callbackhandler, flow_prop, endpointProps,
          new SolFlowEventCallBackHandler());
      recv.start();
    } catch (JCSMPException je) {
      log.info("=========== JCSMP Exception while creating Solace Flow to Queue " + "in SolaceSourceQueueConsumer {} \n",
          je.getLocalizedMessage());
    }
    return true;
  }

  public void stop() {
    if (recv != null) {
      recv.stop();
    }
  }

  public void shutdown() {
    if (recv != null) {
      recv.close();
    }
    if (callbackhandler != null) {
      callbackhandler.shutdown(); // Must remove reference to squeue
    }
  }
}
