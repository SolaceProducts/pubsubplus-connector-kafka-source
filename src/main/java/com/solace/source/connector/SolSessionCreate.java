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

import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;


public class SolSessionCreate {
	private static final Logger log = LoggerFactory.getLogger(SolSessionCreate.class);

	private SolaceSourceConfig lConfig;

	final JCSMPProperties properties = new JCSMPProperties();
	final JCSMPChannelProperties chanProperties = new JCSMPChannelProperties();
	private JCSMPSession session;
	private enum KeyHeader {NONE, DESTINATION, CORRELATION_ID, CORRELATION_ID_AS_BYTES}
	protected KeyHeader keyheader = KeyHeader.NONE;

	public SolSessionCreate(SolaceSourceConfig lConfig) {
		this.lConfig = lConfig;
	}

	public void configureSession() {
		//Required Properties
		properties.setProperty(JCSMPProperties.USERNAME, lConfig.getString(SolaceSourceConstants.SOL_USERNAME));
		properties.setProperty(JCSMPProperties.PASSWORD, lConfig.getString(SolaceSourceConstants.SOL_PASSWORD));
		properties.setProperty(JCSMPProperties.VPN_NAME, lConfig.getString(SolaceSourceConstants.SOL_VPN_NAME));
		properties.setProperty(JCSMPProperties.HOST, lConfig.getString(SolaceSourceConstants.SOL_HOST));

		//Channel Properties
		chanProperties.setConnectTimeoutInMillis(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_connectTimeoutInMillis));
		chanProperties.setReadTimeoutInMillis(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_readTimeoutInMillis));
		chanProperties.setConnectRetries(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_connectRetries ));
		chanProperties.setReconnectRetries(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_reconnectRetries));;
		chanProperties.setConnectRetriesPerHost(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_connectRetriesPerHost));
		chanProperties.setReconnectRetryWaitInMillis(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_reconnectRetryWaitInMillis));
		chanProperties.setKeepAliveIntervalInMillis(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_keepAliveIntervalInMillis));
		chanProperties.setKeepAliveLimit(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_keepAliveLimit));
		chanProperties.setSendBuffer(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_sendBuffer));
		chanProperties.setReceiveBuffer(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_receiveBuffer));
		chanProperties.setTcpNoDelay(lConfig.getBoolean(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_tcpNoDelay));
		chanProperties.setCompressionLevel(lConfig.getInt(SolaceSourceConstants.SOL_CHANNEL_PROPERTY_compressionLevel));
		//Add channel properties to Session Properties
		properties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, chanProperties);

		properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, lConfig.getBoolean(SolaceSourceConstants.SOL_REAPPLY_SUBSCRIPTIONS));
		properties.setBooleanProperty(JCSMPProperties.GENERATE_SEND_TIMESTAMPS, lConfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_SEND_TIMESTAMPS));
		properties.setBooleanProperty(JCSMPProperties.GENERATE_RCV_TIMESTAMPS, lConfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_RCV_TIMESTAMPS));
		properties.setIntegerProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE, lConfig.getInt(SolaceSourceConstants.SOL_SUB_ACK_WINDOW_SIZE));
		properties.setBooleanProperty(JCSMPProperties.GENERATE_SEQUENCE_NUMBERS, lConfig.getBoolean(SolaceSourceConstants.SOL_GENERATE_SEQUENCE_NUMBERS));
		properties.setBooleanProperty(JCSMPProperties.CALCULATE_MESSAGE_EXPIRATION, lConfig.getBoolean(SolaceSourceConstants.SOL_CALCULATE_MESSAGE_EXPIRATION));
		properties.setBooleanProperty(JCSMPProperties.PUB_MULTI_THREAD, lConfig.getBoolean(SolaceSourceConstants.SOL_PUB_MULTI_THREAD));
		properties.setBooleanProperty(JCSMPProperties.MESSAGE_CALLBACK_ON_REACTOR, lConfig.getBoolean(SolaceSourceConstants.SOL_MESSAGE_CALLBACK_ON_REACTOR));
		properties.setBooleanProperty(JCSMPProperties.IGNORE_DUPLICATE_SUBSCRIPTION_ERROR, lConfig.getBoolean(SolaceSourceConstants.SOL_IGNORE_DUPLICATE_SUBSCRIPTION_ERROR));
		properties.setBooleanProperty(JCSMPProperties.IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR, lConfig.getBoolean(SolaceSourceConstants.SOL_IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR));
		properties.setBooleanProperty(JCSMPProperties.NO_LOCAL, lConfig.getBoolean(SolaceSourceConstants.SOL_NO_LOCAL));
		properties.setProperty(JCSMPProperties.AUTHENTICATION_SCHEME, lConfig.getString(SolaceSourceConstants.SOl_AUTHENTICATION_SCHEME));
		properties.setProperty(JCSMPProperties.KRB_SERVICE_NAME, lConfig.getString(SolaceSourceConstants.SOL_KRB_SERVICE_NAME));
		properties.setProperty(JCSMPProperties.SSL_CONNECTION_DOWNGRADE_TO, lConfig.getString(SolaceSourceConstants.SOL_SSL_CONNECTION_DOWNGRADE_TO));
		properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_LOCAL_PRIORITY, lConfig.getInt(SolaceSourceConstants.SOL_SUBSCRIBER_LOCAL_PRIORITY));
		properties.setIntegerProperty(JCSMPProperties.SUBSCRIBER_NETWORK_PRIORITY, lConfig.getInt(SolaceSourceConstants.SOL_SUBSCRIBER_NETWORK_PRIORITY));

		//Use SSL for connection, make sure to use the SSL port for the Solace PubSub+ broker connection URL
		//if((lConfig.getString(SolaceSourceConstants.SOl_AUTHENTICATION_SCHEME)).equals(JCSMPProperties.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE))
		//{
		log.info("=============Attempting to use SSL for PubSub+ connection");
		if(!(lConfig.getString(SolaceSourceConstants.SOL_SSL_CIPHER_SUITES).equals("")))
			properties.setProperty(JCSMPProperties.SSL_CIPHER_SUITES, lConfig.getString(SolaceSourceConstants.SOL_SSL_CIPHER_SUITES));
		properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE, lConfig.getBoolean(SolaceSourceConstants.SOL_SSL_VALIDATE_CERTIFICATE));
		properties.setProperty(JCSMPProperties.SSL_VALIDATE_CERTIFICATE_DATE, lConfig.getBoolean(SolaceSourceConstants.SOL_SSL_VALIDATE_CERTIFICATE_DATE));
		properties.setProperty(JCSMPProperties.SSL_TRUST_STORE, lConfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE));
		properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_PASSWORD, lConfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE_PASSWORD));
		properties.setProperty(JCSMPProperties.SSL_TRUST_STORE_FORMAT, lConfig.getString(SolaceSourceConstants.SOL_SSL_TRUST_STORE_FORMAT));
		properties.setProperty(JCSMPProperties.SSL_TRUSTED_COMMON_NAME_LIST, lConfig.getString(SolaceSourceConstants.SOL_SSL_TRUSTED_COMMON_NAME_LIST));
		properties.setProperty(JCSMPProperties.SSL_KEY_STORE, lConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE));
		properties.setProperty(JCSMPProperties.SSL_KEY_STORE_PASSWORD, lConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_PASSWORD));
		properties.setProperty(JCSMPProperties.SSL_KEY_STORE_FORMAT, lConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_FORMAT));
		properties.setProperty(JCSMPProperties.SSL_KEY_STORE_NORMALIZED_FORMAT, lConfig.getString(SolaceSourceConstants.SOL_SSL_KEY_STORE_NORMALIZED_FORMAT));
		properties.setProperty(JCSMPProperties.SSL_PRIVATE_KEY_PASSWORD, lConfig.getString(SolaceSourceConstants.SOL_SSL_PRIVATE_KEY_PASSWORD));

		//}
	}

	public void connectSession() {

		boolean connected = true;
		try {
			session = JCSMPFactory.onlyInstance().createSession(properties, null, new SolSessionEventCallbackHandler());
		} catch (InvalidPropertiesException e) {
			connected = false;
			log.info("Received Solace excepetion {}, with the following: {} ", e.getCause(), e.getStackTrace());
		}	
		try {
			session.connect();
		} catch (JCSMPException e) {
			log.info("Received Solace excepetion {}, with the following: {} ", e.getCause(), e.getStackTrace());
			connected = false;
		}


		if(connected) {
			log.info("================Session is Connected");
		} else {
			log.info("================Session FAILED to Connect");
		}


	}

	public JCSMPSession getSession() {
		return session;
	}

	public boolean shutdown() {
		if(session != null ) {
			session.closeSession();
		}
		return true;


	}


}
