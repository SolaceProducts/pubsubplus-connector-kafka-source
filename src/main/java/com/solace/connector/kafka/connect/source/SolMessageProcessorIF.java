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

import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface SolMessageProcessorIF {

  Logger log = LoggerFactory.getLogger(SolMessageProcessorIF.class);

  SolMessageProcessorIF process(String skey, BytesXMLMessage message);

  SourceRecord[] getRecords(String kafkaTopic);

  default ConnectHeaders userPropertiesToKafkaHeaders(BytesXMLMessage message) {
    final ConnectHeaders headers = new ConnectHeaders();
    final SDTMap userProperties = message.getProperties();

    if (userProperties != null) {
      for (String key : userProperties.keySet()) {
        try {
          Object value = userProperties.get(key);
          if (value == null) {
            headers.add(key, SchemaAndValue.NULL);
          } else if (value instanceof String) {
            headers.addString(key, (String) value);
          } else if (value instanceof Boolean) {
            headers.addBoolean(key, (Boolean) value);
          } else if (value instanceof byte[]) {
            headers.addBytes(key, (byte[]) value);
          } else if (value instanceof ByteArray) {
            headers.addBytes(key, ((ByteArray) value).asBytes());
          } else if (value instanceof Byte) {
            headers.addByte(key, (byte) value);
          } else if (value instanceof Integer) {
            headers.addInt(key, (Integer) value);
          } else if (value instanceof Short) {
            headers.addShort(key, (Short) value);
          } else if (value instanceof Long) {
            headers.addLong(key, (Long) value);
          } else if (value instanceof Double) {
            headers.addDouble(key, (Double) value);
          } else if (value instanceof Float) {
            headers.addFloat(key, (Float) value);
          } else if (value instanceof BigDecimal) {
            headers.addDecimal(key, (BigDecimal) value);
          } else if (value instanceof BigInteger) {
            headers.addDecimal(key, new BigDecimal((BigInteger) value));
          } else if (value instanceof Date) {
            headers.addDate(key, (Date) value);
          } else if (value instanceof Character) {
            headers.addString(key, ((Character) value).toString());
          } else if (value instanceof Destination) {
            if (log.isTraceEnabled()) {
              log.trace(
                  String.format("Extracting destination name from user property %s", key));
            }
            String destinationName = ((Destination) value).getName();
            headers.addString(key, destinationName);
          } else {
            if (log.isDebugEnabled()) {
              log.debug(String.format("Ignoring user property with key [%s] and type [%s]", key,
                  value.getClass().getName()));
            }
          }
        } catch (SDTException e) {
          log.error(String.format("Ignoring user property with key [%s].", key), e);
        }
      }
    }

    return headers;
  }

  default ConnectHeaders solacePropertiesToKafkaHeaders(BytesXMLMessage msg) {
    final ConnectHeaders headers = new ConnectHeaders();
    if (msg.getApplicationMessageId() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_APPLICATION_MESSAGE_ID,
          msg.getApplicationMessageId());
    }

    if (msg.getApplicationMessageType() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_APPLICATION_MESSAGE_TYPE,
          msg.getApplicationMessageType());
    }

    if (msg.getCorrelationId() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_CORRELATION_ID, msg.getCorrelationId());
    }

    if (msg.getCos() != null) {
      headers.addInt(SolaceSourceConstants.SOL_SH_COS, msg.getCos().value());
    }

    if (msg.getDeliveryMode() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_DELIVERY_MODE, msg.getDeliveryMode().name());
    }

    if (msg.getDestination() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_DESTINATION, msg.getDestination().getName());
    }

    if (msg.getReplyTo() != null) {
      Destination replyToDestination = msg.getReplyTo();
      headers.addString(SolaceSourceConstants.SOL_SH_REPLY_TO_DESTINATION,
          replyToDestination.getName());
      String destinationType = replyToDestination instanceof Topic ? "topic" : "queue";
      headers.addString(SolaceSourceConstants.SOL_SH_REPLY_TO_DESTINATION_TYPE,
          destinationType);
    }

    if (msg.getSenderId() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_SENDER_ID, msg.getSenderId());
    }

    if (msg.getSenderTimestamp() != null) {
      headers.addLong(SolaceSourceConstants.SOL_SH_SENDER_TIMESTAMP, msg.getSenderTimestamp());
    }

    if (msg.getTimeToLive() > 0) {
      headers.addLong(SolaceSourceConstants.SOL_SH_TIME_TO_LIVE, msg.getTimeToLive());
    }

    if (msg.getExpiration() > 0) {
      headers.addLong(SolaceSourceConstants.SOL_SH_EXPIRATION, msg.getExpiration());
    }

    if (msg.getHTTPContentEncoding() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_HTTP_CONTENT_ENCODING,
          msg.getHTTPContentEncoding());
    }

    if (msg.getHTTPContentType() != null) {
      headers.addString(SolaceSourceConstants.SOL_SH_HTTP_CONTENT_TYPE,
          msg.getHTTPContentType());
    }

    if (msg.getSequenceNumber() != null) {
      headers.addLong(SolaceSourceConstants.SOL_SH_SEQUENCE_NUMBER, msg.getSequenceNumber());
    }

    headers.addInt(SolaceSourceConstants.SOL_SH_PRIORITY, msg.getPriority());
    headers.addLong(SolaceSourceConstants.SOL_SH_RECEIVE_TIMESTAMP, msg.getReceiveTimestamp());

    headers.addBoolean(SolaceSourceConstants.SOL_SH_REDELIVERED, msg.getRedelivered());
    headers.addBoolean(SolaceSourceConstants.SOL_SH_DISCARD_INDICATION, msg.getDiscardIndication());
    headers.addBoolean(SolaceSourceConstants.SOL_SH_IS_DMQ_ELIGIBLE, msg.isDMQEligible());
    headers.addBoolean(SolaceSourceConstants.SOL_SH_IS_ELIDING_ELIGIBLE, msg.isElidingEligible());
    headers.addBoolean(SolaceSourceConstants.SOL_SH_IS_REPLY_MESSAGE, msg.isReplyMessage());

    return headers;
  }
}
