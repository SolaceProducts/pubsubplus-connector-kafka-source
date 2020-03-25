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
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SolMessageQueueCallbackHandler implements XMLMessageListener {
  private static final Logger log = LoggerFactory.getLogger(SolMessageQueueCallbackHandler.class);

  private BlockingQueue<BytesXMLMessage> squeue;
  private AtomicInteger msgCounter = new AtomicInteger();

  /**
   * Asynchronously Save records to Blocking Queue when a new Persistent Message arrives.
   * 
   * @param squeue Blocking Queue to hold message events
   */
  public SolMessageQueueCallbackHandler(BlockingQueue<BytesXMLMessage> squeue) {

    this.squeue = squeue;
    msgCounter.set(0);
  }

  @Override
  public void onException(JCSMPException je) {
    log.info("JCSMP Exception in SolMessageQueueProcessorCallback {} \n", je.getLocalizedMessage());

  }

  @Override
  synchronized public void onReceive(BytesXMLMessage msg) {
    log.debug("=================Received Queue Message");
    squeue.add(msg);
  }

  synchronized public void shutdown() {
    squeue = null;
  }

}
