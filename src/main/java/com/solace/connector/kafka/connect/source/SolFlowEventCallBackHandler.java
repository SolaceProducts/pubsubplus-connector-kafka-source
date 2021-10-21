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

import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolFlowEventCallBackHandler implements FlowEventHandler {
  private static final Logger log = LoggerFactory.getLogger(SolFlowEventCallBackHandler.class);

  @Override
  public void handleEvent(Object obj, FlowEventArgs event) {
    log.info("Received Flow (Queue) Event {} with info {} with exception "
        + "{}\n", event.getEvent(), event.getInfo(), event.getException());
    log.info("Source of the Flow (Queue) event: {}\n", obj.getClass().getName());
  }

}
