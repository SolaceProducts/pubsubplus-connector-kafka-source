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
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.solace.source.connector.SolMessageProcessor;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;

public class SolaceSampleKeyedMessageProcessor implements SolMessageProcessor {

	private static final Logger log = LoggerFactory.getLogger(SolaceSampleKeyedMessageProcessor.class);
	private Object sMsg;
	private byte[] messageOut;
	private String sKey;
	private BytesXMLMessage msg;
	private SchemaAndValue key;

	public enum KeyHeader {
		NONE, DESTINATION, CORRELATION_ID, CORRELATION_ID_AS_BYTES
	};

	protected KeyHeader keyheader = KeyHeader.NONE;

	@Override
	public SolMessageProcessor process(String sKey, BytesXMLMessage msg) {
		this.msg = msg;
		this.sKey = sKey.toUpperCase();

		if (msg instanceof TextMessage) {
			;
			log.debug("Text Mesasge received {}", ((TextMessage) msg).getText());
			String sMsg = ((TextMessage) msg).getText();
			messageOut = sMsg.getBytes(StandardCharsets.UTF_8);
		} else {
			log.debug("Message payload: {}", new String(msg.getBytes(), Charset.defaultCharset()));
			if (msg.getBytes().length != 0) { // Binary XML pay load
				messageOut = msg.getBytes();
			} else { // Binary attachment pay load
				messageOut = msg.getAttachmentByteBuffer().array();
			}

		}
		log.debug("Message Dump:{}", msg.dump());

		log.debug("processing data for Kafka topic Key: {}; with message {}", sKey, msg);

		this.sMsg = messageOut;

		if (this.sKey.equals("NONE")) {
			this.keyheader = KeyHeader.NONE;
		} else if (this.sKey.equals("DESTINATION")) {
			this.keyheader = KeyHeader.DESTINATION;
		} else if (this.sKey.equals("CORRELATION_ID")) {
			this.keyheader = KeyHeader.CORRELATION_ID;
		} else if (this.sKey.equals("CORRELATION_ID_AS_BYTES")) {
			this.keyheader = KeyHeader.CORRELATION_ID_AS_BYTES;
		}

		this.key = this.getKey();

		return this;
	}

	@Override
	public SourceRecord[] getRecords(String kafkaTopic) {
		log.debug("=======Key Schema: {}, Key Value: {}", this.key.schema(), this.key.value());
		return new SourceRecord[] { new SourceRecord(null, null, kafkaTopic, null, this.key.schema(), this.key.value(),
				Schema.BYTES_SCHEMA, sMsg) };
	}

	SchemaAndValue getKey() {
		Schema keySchema = null;
		Object key = null;
		String keystr;

		switch (keyheader) {
		case DESTINATION:
			keySchema = Schema.OPTIONAL_STRING_SCHEMA;
			keystr = msg.getDestination().getName();
			key = keystr;
			break;
		case CORRELATION_ID:
			keySchema = Schema.OPTIONAL_STRING_SCHEMA;
			keystr = msg.getCorrelationId();
			key = keystr;
			break;
		case CORRELATION_ID_AS_BYTES:
			keySchema = Schema.OPTIONAL_BYTES_SCHEMA;
			key = msg.getCorrelationId().getBytes(StandardCharsets.UTF_8);
			break;
		case NONE:
			keySchema = null;
			key = null;

		}
		return new SchemaAndValue(keySchema, key);
	}

}
