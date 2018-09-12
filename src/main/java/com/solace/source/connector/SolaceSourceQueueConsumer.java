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

package com.solace.source.connector;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class SolaceSourceQueueConsumer {
  private static final Logger log = LoggerFactory.getLogger(SolaceSourceQueueConsumer.class);
  private SolaceSourceConfig lconfig;
  private BlockingQueue<BytesXMLMessage> squeue;
  private Queue solQueue;
  private FlowReceiver recv;

  // SolaceSourceQueueConsumer(SolaceSourceConfig lconfig,
  // BlockingQueue<SolMessageProcessor> squeue) {
  SolaceSourceQueueConsumer(SolaceSourceConfig lconfig, BlockingQueue<BytesXMLMessage> squeue) {
    this.lconfig = lconfig;
    this.squeue = squeue;
  }

  /**
   * Initializes the JCSMP Session.
   */
  public boolean init(JCSMPSession session) {
    solQueue = JCSMPFactory.onlyInstance()
        .createQueue(lconfig.getString(SolaceSourceConstants.SOl_QUEUE));
    final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
    flow_prop.setEndpoint(solQueue);
    flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
    flow_prop.setStartState(true);
    EndpointProperties endpointProps = new EndpointProperties();
    endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_NONEXCLUSIVE);
    try {

      recv = session.createFlow(new SolMessageQueueCallbackHandler(squeue), 
          flow_prop, 
          endpointProps,
          new SolFlowEventCallBackHandler());
      recv.start();
    } catch (JCSMPException je) {
      log.info("===========JCSMP Exception while creating Solace Flow to Queue "
          + "in SolaceSourceQueueConsumer {} \n",
          je.getLocalizedMessage());
    }

    return true;
  }

  /**
   * Shuts down the Solace Receiver.
   * 
   * <p>@return
   */
  public boolean shutdown() {
    if (recv != null) {
      recv.close();
    }
    return true;

  }
}
