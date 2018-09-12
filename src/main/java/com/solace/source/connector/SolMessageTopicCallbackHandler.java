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
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.XMLMessageListener;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolMessageTopicCallbackHandler implements XMLMessageListener {
  private static final Logger log = LoggerFactory.getLogger(SolMessageTopicCallbackHandler.class);

  private BlockingQueue<BytesXMLMessage> squeue;

  /**
   * Asynchronous Callback for processing Solace Topic data events.
   * 
   * @param lconfig Connector Configuration
   * @param squeue Blocking Queue
   */
  public SolMessageTopicCallbackHandler(SolaceSourceConfig lconfig, 
      BlockingQueue<BytesXMLMessage> squeue) {
    this.squeue = squeue;
    log.debug("===Constructor for SolMessageTopicProcessor");

  }

  @Override
  public void onException(JCSMPException je) {
    log.info("JCSMP Exception in SolaceMessageTopicProcessorCallback "
        + "{} \n", je.getLocalizedMessage());

  }

  @Override
  public void onReceive(BytesXMLMessage msg) {
    log.debug("=================Received Message");

    squeue.add(msg);

  }

}
