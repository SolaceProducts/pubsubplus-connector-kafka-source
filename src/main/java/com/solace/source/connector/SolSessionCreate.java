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

import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolSessionCreate {
  private static final Logger log = LoggerFactory.getLogger(SolSessionCreate.class);

  private SolaceSourceConfig lconfig;

  final JCSMPProperties properties = new JCSMPProperties();
  final JCSMPChannelProperties chanProperties = new JCSMPChannelProperties();
  private JCSMPSession session;

  private enum KeyHeader {
    NONE, DESTINATION, CORRELATION_ID, CORRELATION_ID_AS_BYTES
  }

  protected KeyHeader keyheader = KeyHeader.NONE;

  public SolSessionCreate(SolaceSourceConfig lconfig) {
    this.lconfig = lconfig;
  }

  /**
   * Create JCSMPProperties to configure Solace JCSMPSession.
   */
  public void configureSession() {
    // Required Properties
    properties.setProperty(JCSMPProperties.USERNAME, 
        lconfig.getString(SolaceSourceConstants.SOL_USERNAME));
    properties.setProperty(JCSMPProperties.PASSWORD, 
        lconfig.getString(SolaceSourceConstants.SOL_PASSWORD));
    properties.setProperty(JCSMPProperties.VPN_NAME, 
        lconfig.getString(SolaceSourceConstants.SOL_VPN_NAME));
    properties.setProperty(JCSMPProperties.HOST, lconfig.getString(SolaceSourceConstants.SOL_HOST));

    // Channel Properties
    chanProperties.setConnectTimeoutInMillis(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_connectTimeoutInMillis));
    chanProperties.setReadTimeoutInMillis(lconfig
        .getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_readTimeoutInMillis));
    chanProperties.setConnectRetries(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_connectRetries));
    chanProperties.setReconnectRetries(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_reconnectRetries));
    ;
    chanProperties.setConnectRetriesPerHost(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_connectRetriesPerHost));
    chanProperties.setReconnectRetryWaitInMillis(
        lconfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_reconnectRetryWaitInMillis));
    chanProperties.setKeepAliveIntervalInMillis(
        lconfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_keepAliveIntervalInMillis));
    chanProperties.setKeepAliveLimit(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_keepAliveLimit));
    chanProperties.setSendBuffer(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_sendBuffer));
    chanProperties.setReceiveBuffer(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_receiveBuffer));
    chanProperties.setTcpNoDelay(lconfig.getBoolean(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_tcpNoDelay));
    chanProperties.setCompressionLevel(lconfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_compressionLevel));
    // Add channel properties to Session Properties
    properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, chanProperties);

    properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS,
        lconfig.getBoolean(SolaceSourceConstants.SOL_REAPPLY_SUBSCRIPTIONS));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS,
        lconfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_SEND_TIMESTAMPS));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS,
        lconfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_RCV_TIMESTAMPS));
    properties.setIntegerProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE,
        lconfig.getInt(SolaceSourceConstants.SOL_SUB_ACK_WINDOW_SIZE));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS,
        lconfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_SEQUENCE_NUMBERS));
    properties.setBooleanProperty(JCSMPProperties.CALCULATE_MESSAGE_EXPIRATION,
        lconfig.getBoolean(SolaceSourceConstants.SOL_CALCULATE_MESSAGE_EXPIRATION));
    properties.setBooleanProperty(JCSMPProperties.PUB_MULTI_THREAD,
        lconfig.getBoolean(SolaceSourceConstants.SOL_PUB_MULTI_THREAD));
    properties.setBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR,
        lconfig.getBoolean(SolaceSourceConstants.SOL_MESSAGE_CALLBACK_ON_REACTOR));
    properties.setBooleanProperty(JCSMPProperties.IGNORE_DUPLICATE_SUBSCRIPTION_ERROR,
        lconfig.getBoolean(SolaceSourceConstants.SOL_IGNORE_DUPLICATE_SUBSCRIPTION_ERROR));
    properties.setBooleanProperty(JCSMPProperties.IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR,
        lconfig.getBoolean(SolaceSourceConstants.SOL_IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR));
    properties.setBooleanProperty(JCSMPProperties
        .NO_LOCAL, lconfig.getBoolean(SolaceSourceConstants.SOL_NO_LOCAL));
    properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
        lconfig.getString(SolaceSourceConstants.SOl_AUTHENTICATION_SCHEME));
    properties.setProperty(JCSMPProperties.KRB_SERVICE_NAME,
        lconfig.getString(SolaceSourceConstants.SOL_KRB_SERVICE_NAME));
    properties.setProperty(JCSMPProperties.SSL_CONNECTION_DOWNGRADE_TO,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_CONNECTION_DOWNGRADE_TO));
    properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_LOCAL_PRIORITY,
        lconfig.getInt(SolaceSourceConstants.SOL_SUBSCRIBER_LOCAL_PRIORITY));
    properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_NETWORK_PRIORITY,
        lconfig.getInt(SolaceSourceConstants.SOL_SUBSCRIBER_NETWORK_PRIORITY));

    // Use SSL for connection, make sure to use the SSL port for the Solace PubSub+
    // broker connection URL
    log.info("=============Attempting to use SSL for PubSub+ connection");
    if (!(lconfig.getString(SolaceSourceConstants.SOL_SSL_CIPHER_SUITES).equals(""))) {
      properties.setProperty(JCSMPProperties.SSL_CIPHER_SUITES,
          lconfig.getString(SolaceSourceConstants.SOL_SSL_CIPHER_SUITES));
    }
    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE,
        lconfig.getBoolean(SolaceSourceConstants.SOL_SSL_VALIDATE_CERTIFICATE));
    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE,
        lconfig.getBoolean(SolaceSourceConstants.SOL_SSL_VALIDATE_CERTIFICATE_DATE));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE_PASSWORD));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_TRUSTED_COMMON_NAME_LIST,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_TRUSTED_COMMON_NAME_LIST));
    properties.setProperty(JCSMPProperties
        .SSL_KEY_STORE, lconfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_PASSWORD));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_NORMALIZED_FORMAT,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_NORMALIZED_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD,
        lconfig.getString(SolaceSourceConstants.SOL_SSL_PRIVATE_KEY_PASSWORD));

    // }
  }

  /**
   * Connect JCSMPSession.
   * @return boolean result
   */
  public boolean connectSession() {

    boolean connected = false;
    try {
      session = JCSMPFactory.onlyInstance(
          ).createSession(properties, null, new SolSessionEventCallbackHandler());
    } catch (InvalidPropertiesException e) {
      connected = false;
      log.info("Received Solace excepetion {}, with the "
          + "following: {} ", e.getCause(), e.getStackTrace());
    }
    try {
      session.connect();
      connected = true;
    } catch (JCSMPException e) {
      log.info("Received Solace excepetion {}, with the "
          + "following: {} ", e.getCause(), e.getStackTrace());
      connected = false;
    }
    return connected;

  }

  public JCSMPSession getSession() {
    return session;
  }

  /**
   * Shutdown the session.
   * @return return shutdown boolean result
   */
  public boolean shutdown() {
    if (session != null) {
      session.closeSession();
    }
    return true;

  }

}
