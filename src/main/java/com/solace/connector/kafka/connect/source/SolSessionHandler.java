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

import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPSessionStats;
import com.solacesystems.jcsmp.statistics.StatType;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;
import java.util.Optional;

public class SolSessionHandler {
  private static final Logger log = LoggerFactory.getLogger(SolSessionHandler.class);

  private SolaceSourceConnectorConfig connectorConfig;

  final JCSMPProperties properties = new JCSMPProperties();
  final JCSMPChannelProperties chanProperties = new JCSMPChannelProperties();
  private JCSMPSession session;
  private Context ctx;

  public SolSessionHandler(SolaceSourceConnectorConfig connectorConfig) {
    this.connectorConfig = connectorConfig;
    ContextProperties ctx_prop = new ContextProperties();
    ctx_prop.setName(Thread.currentThread().getName());  // unique name
    ctx = JCSMPFactory.onlyInstance().createContext(ctx_prop);
  }

  /**
   * Create JCSMPProperties to configure Solace JCSMPSession.
   */
  public void configureSession() {
    // Required Properties
    properties.setProperty(JCSMPProperties.USERNAME,
        connectorConfig.getString(SolaceSourceConstants.SOL_USERNAME));
    properties.setProperty(JCSMPProperties.PASSWORD,
            Optional.ofNullable(connectorConfig.getPassword(SolaceSourceConstants.SOL_PASSWORD))
                    .map(Password::value).orElse(null));
    properties.setProperty(JCSMPProperties.VPN_NAME,
        connectorConfig.getString(SolaceSourceConstants.SOL_VPN_NAME));
    properties.setProperty(JCSMPProperties.HOST, connectorConfig.getString(SolaceSourceConstants.SOL_HOST));

    // Channel Properties
    chanProperties.setConnectTimeoutInMillis(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_connectTimeoutInMillis));
    chanProperties.setReadTimeoutInMillis(connectorConfig
        .getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_readTimeoutInMillis));
    chanProperties.setConnectRetries(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_connectRetries));
    chanProperties.setReconnectRetries(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_reconnectRetries));
    ;
    chanProperties.setConnectRetriesPerHost(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_connectRetriesPerHost));
    chanProperties.setReconnectRetryWaitInMillis(
        connectorConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_reconnectRetryWaitInMillis));
    chanProperties.setKeepAliveIntervalInMillis(
        connectorConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_keepAliveIntervalInMillis));
    chanProperties.setKeepAliveLimit(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_keepAliveLimit));
    chanProperties.setSendBuffer(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_sendBuffer));
    chanProperties.setReceiveBuffer(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_receiveBuffer));
    chanProperties.setTcpNoDelay(connectorConfig.getBoolean(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_tcpNoDelay));
    chanProperties.setCompressionLevel(connectorConfig.getInt(SolaceSourceConstants
        .SOL_CHANNEL_PROPERTY_compressionLevel));
    // Add channel properties to Session Properties
    properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, chanProperties);

    properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_REAPPLY_SUBSCRIPTIONS));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_SEND_TIMESTAMPS));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_RCV_TIMESTAMPS));
    properties.setIntegerProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE,
        connectorConfig.getInt(SolaceSourceConstants.SOL_SUB_ACK_WINDOW_SIZE));
    properties.setBooleanProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_SEQUENCE_NUMBERS));
    properties.setBooleanProperty(JCSMPProperties.CALCULATE_MESSAGE_EXPIRATION,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_CALCULATE_MESSAGE_EXPIRATION));
    properties.setBooleanProperty(JCSMPProperties.PUB_MULTI_THREAD,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_PUB_MULTI_THREAD));
    properties.setBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_MESSAGE_CALLBACK_ON_REACTOR));
    properties.setBooleanProperty(JCSMPProperties.IGNORE_DUPLICATE_SUBSCRIPTION_ERROR,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_IGNORE_DUPLICATE_SUBSCRIPTION_ERROR));
    properties.setBooleanProperty(JCSMPProperties.IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR));
    properties.setBooleanProperty(JCSMPProperties
        .NO_LOCAL, connectorConfig.getBoolean(SolaceSourceConstants.SOL_NO_LOCAL));
    properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME,
        connectorConfig.getString(SolaceSourceConstants.SOL_AUTHENTICATION_SCHEME));
    properties.setProperty(JCSMPProperties.KRB_SERVICE_NAME,
        connectorConfig.getString(SolaceSourceConstants.SOL_KRB_SERVICE_NAME));
    properties.setProperty(JCSMPProperties.SSL_CONNECTION_DOWNGRADE_TO,
        connectorConfig.getString(SolaceSourceConstants.SOL_SSL_CONNECTION_DOWNGRADE_TO));
    properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_LOCAL_PRIORITY,
        connectorConfig.getInt(SolaceSourceConstants.SOL_SUBSCRIBER_LOCAL_PRIORITY));
    properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_NETWORK_PRIORITY,
        connectorConfig.getInt(SolaceSourceConstants.SOL_SUBSCRIBER_NETWORK_PRIORITY));

    // Use SSL for connection, make sure to use the SSL port for the Solace PubSub+
    // broker connection URL
    log.info("=============Attempting to use SSL for PubSub+ connection");
    if (!(connectorConfig.getString(SolaceSourceConstants.SOL_SSL_CIPHER_SUITES).equals(""))) {
      properties.setProperty(JCSMPProperties.SSL_CIPHER_SUITES,
          connectorConfig.getString(SolaceSourceConstants.SOL_SSL_CIPHER_SUITES));
    }
    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_SSL_VALIDATE_CERTIFICATE));
    properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE,
        connectorConfig.getBoolean(SolaceSourceConstants.SOL_SSL_VALIDATE_CERTIFICATE_DATE));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE,
        connectorConfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD,
            Optional.ofNullable(connectorConfig.getPassword(SolaceSourceConstants.SOL_SSL_TRUST_STORE_PASSWORD))
                    .map(Password::value).orElse(null));
    properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT,
        connectorConfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_TRUSTED_COMMON_NAME_LIST,
        connectorConfig.getString(SolaceSourceConstants.SOL_SSL_TRUSTED_COMMON_NAME_LIST));
    properties.setProperty(JCSMPProperties
        .SSL_KEY_STORE, connectorConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD,
            Optional.ofNullable(connectorConfig.getPassword(SolaceSourceConstants.SOL_SSL_KEY_STORE_PASSWORD))
                    .map(Password::value).orElse(null));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT,
        connectorConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_KEY_STORE_NORMALIZED_FORMAT,
        connectorConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_NORMALIZED_FORMAT));
    properties.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD,
            Optional.ofNullable(connectorConfig.getPassword(SolaceSourceConstants.SOL_SSL_PRIVATE_KEY_PASSWORD))
                    .map(Password::value).orElse(null));

    // }
  }

  /**
   * Connect JCSMPSession.
   * @throws JCSMPException In case of JCSMP error
   */
  public void connectSession() throws JCSMPException {

    System.setProperty("java.security.auth.login.config",
        connectorConfig.getString(SolaceSourceConstants.SOL_KERBEROS_LOGIN_CONFIG));
    System.setProperty("java.security.krb5.conf",
        connectorConfig.getString(SolaceSourceConstants.SOL_KERBEROS_KRB5_CONFIG));

    session = JCSMPFactory.onlyInstance().createSession(properties, ctx, new SolSessionEventCallbackHandler());
    session.connect();
  }

  public JCSMPSession getSession() {
    return session;
  }

  public void printStats() {
    if (session != null) {
      JCSMPSessionStats lastStats = session.getSessionStats();
      Enumeration<StatType> estats = StatType.elements();
      while (estats.hasMoreElements()) {
        StatType statName = estats.nextElement();
        log.info("\t" + statName.getLabel() + ": " + lastStats.getStat(statName));
      }
      log.info("\n");
    }
  }

  /**
   * Shutdown the session.
   * @return return shutdown boolean result
   */
  public boolean shutdown() {

    if ( session != null ) {
      session.closeSession();
    }
    if ( ctx != null ) {
      ctx.destroy();
    }
    session = null;
    return true;
  }
}
