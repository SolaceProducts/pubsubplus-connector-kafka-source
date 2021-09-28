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

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPReconnectEventHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolReconnectCallbackHandler implements JCSMPReconnectEventHandler {
  private static final Logger log = LoggerFactory.getLogger(SolReconnectCallbackHandler.class);

  @Override
  public void postReconnect() throws JCSMPException {
    log.info("========Solace client now Reconnected after Solace HA or DR fail-over");
  }

  @Override
  public boolean preReconnect() throws JCSMPException {
    log.info(
        "=========Solace client now in Pre Reconnect state, trying to re-establish a connection"
        + " -- possibly due to Solace HA or DR fail-over");
    return true;
  }

}
