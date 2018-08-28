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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;

public class SolSessionEventCallbackHandler implements SessionEventHandler{
	final Logger log = LoggerFactory.getLogger(SolSessionEventCallbackHandler.class);

	@Override
	public void handleEvent(SessionEventArgs event) {

		log.info("Received Session Event {} with info {}\n", event.getEvent(), event.getInfo());

		// Received event possibly due to DR fail-over complete
		if(event.getEvent() == SessionEvent.VIRTUAL_ROUTER_NAME_CHANGED) {
			log.info("Looks like DR fail-over may have just occured and has completed successfully");
		}

	}


}
