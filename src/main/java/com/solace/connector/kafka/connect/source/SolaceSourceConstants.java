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

/**
 * SolaceSourceConstants is responsible for correct configuration management.
 * Refer to
 * https://docs.solace.com/API-Developer-Online-Ref-Documentation/java/index.html
 * for more details under JCSMPProperties.
 */
public class SolaceSourceConstants {

  // High Importance Kafka
  public static final String KAFKA_TOPIC = "kafka.topic";

  // High Importance Solace Message processor
  public static final String SOL_MESSAGE_PROCESSOR = "sol.message_processor_class";

  // High Importance Solace
  public static final String SOL_HOST = "sol.host";
  public static final String SOL_USERNAME = "sol.username";
  public static final String SOL_PASSWORD = "sol.password";
  
  // TODO: SOL_MESSAGE_ACK_MODE is not used!
  public static final String SOL_MESSAGE_ACK_MODE = "sol.message_ack_mode";
  
  public static final String SOL_VPN_NAME = "sol.vpn_name";
  public static final String SOL_TOPICS = "sol.topics";
  public static final String SOL_QUEUE = "sol.queue";

  // Low Importance General Properties
  public static final String SOL_SESSION_NAME = "sol.session_name";
  public static final String SOL_LOCALHOST = "sol.localhost";
  public static final String SOL_CLIENT_NAME = "sol.client_name";
  public static final String SOL_GENERATE_SENDER_ID = "sol.generate_sender_id";
  public static final String SOL_GENERATE_RCV_TIMESTAMPS = "sol.generate_rcv_timestamps";
  public static final String SOL_GENERATE_SEND_TIMESTAMPS = "sol.generate_send_timestamps";
  public static final String SOL_GENERATE_SEQUENCE_NUMBERS = "sol.generate_sequence_numbers";
  public static final String SOL_CALCULATE_MESSAGE_EXPIRATION = "sol.calculate_message_expiration";
  public static final String SOL_REAPPLY_SUBSCRIPTIONS = "sol.reapply_subscriptions";
  public static final String SOL_PUB_MULTI_THREAD = "sol.pub_multi_thread";
  public static final String SOL_PUB_USE_INTERMEDIATE_DIRECT_BUF 
      = "sol.pub_use_immediate_direct_pub";
  public static final String SOL_MESSAGE_CALLBACK_ON_REACTOR = "sol.message_callback_on_reactor";
  public static final String SOL_IGNORE_DUPLICATE_SUBSCRIPTION_ERROR 
      = "sol.ignore_duplicate_subscription_error";
  public static final String SOL_IGNORE_SUBSCRIPTION_NOT_FOUND_ERROR 
      = "sol.ignore_subscription_not_found_error";
  public static final String SOL_NO_LOCAL = "sol.no_local";
  public static final String SOL_ACK_EVENT_MODE = "sol.ack_event_mode";
  public static final String SOL_AUTHENTICATION_SCHEME = "sol.authentication_scheme";
  public static final String SOL_KRB_SERVICE_NAME = "sol.krb_service_name";
  public static final String SOL_SSL_CONNECTION_DOWNGRADE_TO = "sol.ssl_connection_downgrade_to";

  // Low Importance Solace TLS Protocol properties
  // public static final String SOL_SSL_PROTOCOL = "sol.ssl_protocol";
  public static final String SOL_SSL_EXCLUDED_PROTOCOLS = "sol.ssl_excluded_protocols";
  public static final String SOL_SSL_CIPHER_SUITES = "sol.ssl_cipher_suites";
  public static final String SOL_SSL_VALIDATE_CERTIFICATE = "sol.ssl_validate_certificate";
  public static final String SOL_SSL_VALIDATE_CERTIFICATE_DATE = "sol.ssl_validate_certicate_date";
  public static final String SOL_SSL_TRUST_STORE = "sol.ssl_trust_store";
  public static final String SOL_SSL_TRUST_STORE_PASSWORD = "sol.ssl_trust_store_password";
  public static final String SOL_SSL_TRUST_STORE_FORMAT = "sol.ssl_trust_store_format";
  public static final String SOL_SSL_TRUSTED_COMMON_NAME_LIST = "sol.ssl_trusted_common_name_list";
  public static final String SOL_SSL_KEY_STORE = "sol.ssl_key_store";
  public static final String SOL_SSL_KEY_STORE_PASSWORD = "sol.ssl_key_store_password";
  public static final String SOL_SSL_KEY_STORE_FORMAT = "sol.ssl_key_store_format";
  public static final String SOL_SSL_KEY_STORE_NORMALIZED_FORMAT 
      = "sol.ssl_key_store_normalized_format";
  public static final String SOL_SSL_PRIVATE_KEY_ALIAS = "sol.ssl_private_key_alias";
  public static final String SOL_SSL_PRIVATE_KEY_PASSWORD = "sol.ssl_private_key_password";

  // Low Importance Solace Channel Properties
  public static final String SOL_CHANNEL_PROPERTY_connectTimeoutInMillis 
      = "sol.channel_properties.connect_timout_in_millis";
  public static final String SOL_CHANNEL_PROPERTY_readTimeoutInMillis 
      = "sol.channel_properties.read_timeout_in_millis";
  public static final String SOL_CHANNEL_PROPERTY_connectRetries 
      = "sol.channel_properties.connect_retries";
  public static final String SOL_CHANNEL_PROPERTY_reconnectRetries 
      = "sol.channel_properties.reconnect_retries";
  public static final String SOL_CHANNEL_PROPERTY_connectRetriesPerHost 
      = "sol.channnel_properties.connect_retries_per_host";
  public static final String SOL_CHANNEL_PROPERTY_reconnectRetryWaitInMillis 
      = "sol.channel_properties.reconnect_retry_wait_in_millis";
  public static final String SOL_CHANNEL_PROPERTY_keepAliveIntervalInMillis 
      = "sol.channel_properties.keep_alive_interval_in_millis";
  public static final String SOL_CHANNEL_PROPERTY_keepAliveLimit 
      = "sol.channel_properties.keep_alive_limit";
  public static final String SOL_CHANNEL_PROPERTY_sendBuffer = "sol.channel_properties.send_buffer";
  public static final String SOL_CHANNEL_PROPERTY_receiveBuffer 
      = "sol.channel_properties.receive_buffer";
  public static final String SOL_CHANNEL_PROPERTY_tcpNoDelay 
      = "sol.channel_properties.tcp_no_delay";
  public static final String SOL_CHANNEL_PROPERTY_compressionLevel 
      = "sol.channel_properties.compression_level";

  // Low Importance Persistent Message Properties
  public static final String SOL_SUB_ACK_WINDOW_SIZE = "sol.sub_ack_window_size";
  public static final String SOL_PUB_ACK_WINDOW_SIZE = "sol.pub_ack_window_size";
  public static final String SOL_SUB_ACK_TIME = "sol.sub_ack_time";
  public static final String SOL_PUB_ACK_TIME = "sol.pub_ack_time";
  public static final String SOL_SUB_ACK_WINDOW_THRESHOLD = "sol.sub_ack_window_threshold";
  public static final String SOL_MAX_RESENDS = "sol.max_resends";
  public static final String SOL_GD_RECONNECT_FAIL_ACTION = "sol.gd_reconnect_fail_action";

  // Low importance DTO (Deliver To One) Properties
  public static final String SOL_SUBSCRIBER_LOCAL_PRIORITY = "sol.susbcriber_local_priority";
  public static final String SOL_SUBSCRIBER_NETWORK_PRIORITY = "sol.susbcriber_network_priority";
  public static final String SOL_SUBSCRIBER_DTO_OVERRIDE = "sol.subscriber_dto_override";

  // Low importance Kafka key
  // Allowable values include: NONE, DESTINATION, CORRELATION_ID,
  // CORRELATION_ID_AS_BYTES
  public static final String SOL_KAFKA_MESSAGE_KEY = "sol.kafka_message_key";
  
  //Low importance Kerberos details
  public static final String SOL_KERBEROS_LOGIN_CONFIG = "sol.kerberos.login.conf";
  public static final String SOL_KERBEROS_KRB5_CONFIG = "sol.kerberos.krb5.conf";  
  

}
