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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.TopicProperties;
import com.solacesystems.jcsmp.XMLMessageConsumer;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceSourceTopicListener {

  private static final Logger log = LoggerFactory.getLogger(SolaceSourceTopicListener.class);

  private SolaceSourceConnectorConfig lconfig;

  private String solaceTopics;
  private String[] topics;
  private XMLMessageConsumer cons;
  SolMessageTopicCallbackHandler callbackhandler;

  public SolaceSourceTopicListener(SolaceSourceConnectorConfig lconfig) {
    this.lconfig = lconfig;
  }

  /**
   * Initializes JCSMPSession.
   */
  public boolean init(JCSMPSession session, BlockingQueue<BytesXMLMessage> squeue) {

    boolean topicListenerStarted = true;
    solaceTopics = lconfig.getString(SolaceSourceConstants.SOL_TOPICS);
    topics = solaceTopics.split(",");

    try {
      callbackhandler = new SolMessageTopicCallbackHandler(lconfig, squeue);
      cons = session.getMessageConsumer(new SolReconnectCallbackHandler(),
          callbackhandler );
    } catch (JCSMPException je) {
      log.info("JCSMP Exception in SolaceSourceTopicListener {} \n", je.getLocalizedMessage());
    }
    try {
      Topic topic;
      int counter = 0;
      log.info("Number of topics to add: {} ", topics.length);
      while (topics.length > counter) {
        log.info("Adding subscription for topic {} ", topics[counter].trim());
        TopicProperties tproperties = new TopicProperties();
        tproperties.setRxAllDeliverToOne(lconfig
            .getBoolean(SolaceSourceConstants.SOL_SUBSCRIBER_DTO_OVERRIDE));
        tproperties.setName(topics[counter].trim());
        topic = JCSMPFactory.onlyInstance().createTopic(tproperties);
        session.addSubscription(topic, true);
        counter++;
      }
    } catch (JCSMPException je) {
      log.info("JCSMP Exception in SolaceSourceTopicListener {} \n", je.getLocalizedMessage());
    }

    try {
      cons.start();
    } catch (JCSMPException je) {
      log.info("JCSMP Exception in SolaceSourceTopicListener {} \n", je.getLocalizedMessage());
      topicListenerStarted = false;
    }

    log.info("================Session is Connected");
    return topicListenerStarted;

  }

  /**
   * Stops JCSMPSession.
   * @return
   */
  public boolean shutdown() {
    if (cons != null) {
      cons.close();
    }
    if (callbackhandler != null ) {
      callbackhandler.shutdown(); // Must remove reference to squeue
    }
    return true;
  }

}
