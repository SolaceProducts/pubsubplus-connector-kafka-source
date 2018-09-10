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

package com.solace.source.connector.msgProcessors;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solace.source.connector.SolMessageProcessor;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.TextMessage;


public class SolSampleSimpleMessageProcessor implements SolMessageProcessor{

	private static final Logger log = LoggerFactory.getLogger(SolSampleSimpleMessageProcessor.class);
	private Object sMsg;
	private String sKey;
	private Object sDestination;
	private byte[] messageOut;
	private BytesXMLMessage msg;


	@Override
	public SolMessageProcessor process(String sKey, BytesXMLMessage msg) {
		this.sMsg = msg;
		if (msg instanceof TextMessage) {
		log.debug("Text Mesasge received {}", ((TextMessage)msg).getText());
		String sMsg = ((TextMessage) msg).getText();
		messageOut = sMsg.getBytes(StandardCharsets.UTF_8);
		} else {
			log.debug("Message payload: {}", new String(msg.getBytes(), Charset.defaultCharset()));
			if(msg.getBytes().length != 0) { //Binary XML pay load
				messageOut = msg.getBytes();
			} else { // Binary attachment pay load
				messageOut = msg.getAttachmentByteBuffer().array();
			}

		}
		log.debug("Message Dump:{}", msg.dump());

		this.sDestination = msg.getDestination().getName();
		log.debug("processing data for destination: {}; with message {}, with Kafka topic key of: {}", (String)this.sDestination, msg, this.sKey);
		this.sKey = sKey;
		this.sMsg = messageOut;
		return this;
	}

	@Override
	public SourceRecord[] getRecords(String kafkaTopic) {
		if(msg.getDeliveryMode() == DeliveryMode.PERSISTENT || msg.getDeliveryMode() == DeliveryMode.NON_PERSISTENT)
			this.msg.ackMessage();//closest we can get to actual write to Kafka, now ack to remove message from Solace Queue since we are done.
		return new SourceRecord[]{new SourceRecord(null, null, kafkaTopic, null,
				Schema.STRING_SCHEMA, sDestination,
				Schema.BYTES_SCHEMA, sMsg)};
	}

}
