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

package com.solace.connector.kafka.connect.source.msgprocessors;

import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_MAP_SOLACE_STANDARD_PROPERTIES;
import static com.solace.connector.kafka.connect.source.SolaceSourceConstants.SOL_MESSAGE_PROCESSOR_MAP_USER_PROPERTIES;
import com.solace.connector.kafka.connect.source.SolMessageProcessorIF;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSampleKeyedMessageProcessor implements SolMessageProcessorIF, Configurable {

  private static final Logger log
      = LoggerFactory.getLogger(SolaceSampleKeyedMessageProcessor.class);
  private Object smsg;
  private Object sdestination;
  private byte[] messageOut;
  private String skey;
  private BytesXMLMessage msg;
  private SchemaAndValue key;
  private LinkedList<Header> headers = new LinkedList<>();

  public enum KeyHeader {
    NONE, DESTINATION, CORRELATION_ID, CORRELATION_ID_AS_BYTES
  }

  protected KeyHeader keyheader = KeyHeader.NONE;

  private Map<String, ?> configs;
  private boolean mapUserProperties;
  private boolean mapSolaceStandardProperties;

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
    this.mapUserProperties = getBooleanConfigProperty(SOL_MESSAGE_PROCESSOR_MAP_USER_PROPERTIES);
    this.mapSolaceStandardProperties = getBooleanConfigProperty(
        SOL_MESSAGE_PROCESSOR_MAP_SOLACE_STANDARD_PROPERTIES);
  }

  @Override
  public SolMessageProcessorIF process(String skey, BytesXMLMessage msg) {
    this.msg = msg;
    this.headers.clear();
    this.skey = skey.toUpperCase();

    if (log.isDebugEnabled()) {
      log.debug("{} received.", msg.getClass().getName());
    }
    if (msg instanceof TextMessage) {
      String smsg = ((TextMessage) msg).getText();
      messageOut = smsg.getBytes(StandardCharsets.UTF_8);
    } else {
      if (msg.getBytes().length != 0) { // Binary XML pay load
        messageOut = msg.getBytes();
      } else { // Binary attachment pay load
        messageOut = msg.getAttachmentByteBuffer().array();
      }
    }

    this.sdestination = msg.getDestination().getName();
    if (log.isDebugEnabled()) {
      log.debug("processing data for destination: {}; with Kafka topic key of: {}",
          this.sdestination, this.skey);
    }

    this.smsg = messageOut;

    if (this.skey.equals("NONE")) {
      this.keyheader = KeyHeader.NONE;
    } else if (this.skey.equals("DESTINATION")) {
      this.keyheader = KeyHeader.DESTINATION;
    } else if (this.skey.equals("CORRELATION_ID")) {
      this.keyheader = KeyHeader.CORRELATION_ID;
    } else if (this.skey.equals("CORRELATION_ID_AS_BYTES")) {
      this.keyheader = KeyHeader.CORRELATION_ID_AS_BYTES;
    }

    this.key = this.getKey();

    if (mapUserProperties) {
      ConnectHeaders userProps = userPropertiesToKafkaHeaders(msg);
      userProps.iterator().forEachRemaining(headers::add);
    }

    if (mapSolaceStandardProperties) {
      ConnectHeaders solaceProps = solacePropertiesToKafkaHeaders(msg);
      solaceProps.iterator().forEachRemaining(headers::add);
    }
    return this;
  }

  @Override
  public SourceRecord[] getRecords(String kafkaTopic) {
    if (log.isDebugEnabled()) {
      log.debug("=======Key Schema: {}, Key Value: {}", this.key.schema(), this.key.value());
    }
    return new SourceRecord[]{new SourceRecord(null, null, kafkaTopic,
        null, this.key.schema(), this.key.value(),
        Schema.BYTES_SCHEMA, smsg, null, headers)};
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
        break;
      default:
        keySchema = null;
        key = null;

    }
    return new SchemaAndValue(keySchema, key);
  }

  private boolean getBooleanConfigProperty(String name) {
    if (this.configs != null && this.configs.containsKey(name)) {
      final Object value = this.configs.get(name);
      if (value instanceof String) {
        return Boolean.parseBoolean((String) value);
      } else if (value instanceof Boolean) {
        return (boolean) value;
      } else {
        log.error("The value of property {} should be of type boolean or string.", name);
      }
    }
    return false;
  }
}
