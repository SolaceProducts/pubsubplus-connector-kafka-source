[![Actions Status](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/workflows/build/badge.svg?branch=master)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions?query=workflow%3Abuild+branch%3Amaster)
[![Code Analysis (CodeQL)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions/workflows/codeql-analysis.yml/badge.svg?branch=master)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions/workflows/codeql-analysis.yml)
[![Code Analysis (PMD)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions/workflows/pmd-analysis.yml/badge.svg?branch=master)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions/workflows/pmd-analysis.yml)
[![Code Analysis (SpotBugs)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions/workflows/spotbugs-analysis.yml/badge.svg?branch=master)](https://github.com/SolaceProducts/pubsubplus-connector-kafka-source/actions/workflows/spotbugs-analysis.yml)

# Solace PubSub+ Connector for Kafka: Source

This project provides a Solace PubSub+ Event Broker to Kafka [Source Connector](//kafka.apache.org/documentation.html#connect_concepts) (adapter) that makes use of the [Kafka Connect API](//kafka.apache.org/documentation/#connect).

**Note**: there is also a PubSub+ Kafka Sink Connector available from the [Solace PubSub+ Connector for Kafka: Sink](https://github.com/SolaceProducts/pubsubplus-connector-kafka-sink) GitHub repository.

Contents:

  * [Overview](#overview)
  * [Use Cases](#use-cases)
  * [Downloads](#downloads)
  * [Quick Start](#quick-start)
  * [Parameters](#parameters)
  * [User Guide](#user-guide)
    + [Deployment](#deployment)
    + [Troubleshooting](#troubleshooting)
    + [Message Processors](#message-processors)
    + [Performance Considerations](#performance-considerations)
    + [Security Considerations](#security-considerations)  
  * [Developers Guide](#developers-guide)

## Overview

The PubSub+ Kafka Source Connector consumes PubSub+ event broker real-time queue or topic data events and streams them to a Kafka topic as Source Records. 

The connector was created using PubSub+ high performance Java API to move PubSub+ data events to the Kafka Broker.

## Use Cases

#### Protocol and API Messaging Transformations

Unlike many other message brokers, the Solace PubSub+ Event Broker supports transparent protocol and API messaging transformations.

As the following diagram shows, any message that reaches the PubSub+ broker via the many supported message transports or language/API (examples can include an iPhone via C API, a REST POST, an AMQP, JMS or MQTT message) can be moved to a Topic (Key or not Keyed) on the Kafka broker via the single PubSub+ Source Connector.

![Messaging Transformations](/doc/images/KSource.png)

#### Tying Kafka into the PubSub+ Event Mesh

The [PubSub+ Event Mesh](//docs.solace.com/Solace-PubSub-Platform.htm#PubSub-mesh) is a clustered group of PubSub+ Event Brokers, which appears to individual services (consumers or producers of data events) to be a single transparent event broker. The Event Mesh routes data events in real-time to any of its client services. The Solace PubSub+ brokers can be any of the three categories: dedicated extreme performance hardware appliances, high performance software brokers that are deployed as software images (deployable under most Hypervisors, Cloud IaaS and PaaS layers and in Docker) or provided as a fully-managed Cloud MaaS (Messaging as a Service). 

Simply by having the PubSub+ Source Connector register interest in receiving events, the entire Event Mesh becomes aware of the registration request and will know how to securely route the appropriate events generated by other service on the Event Mesh to the PubSub+ Source Connector. The PubSub+ Source Connector takes those event  messages and sends them as Kafka Source Records to the Kafka broker for storage in a Kafka Topic, regardless where in the Service Mesh the service is located that generated the event.

![Messaging Transformations](/doc/images/EventMesh.png)

#### Eliminating the Need for Separate Source Connectors

The PubSub+ Source Connector eliminates the complexity and overhead of maintaining separate Source Connectors for each and every upstream service that generates data events that Kafka may wish to consume. There is the added benefit of access to services where there is no Kafka Source Connector available, thereby eliminating the need to create and maintain a new connector for services from which Kafka may wish to store the data.

![Messaging Transformations](/doc/images/SingleConnector.png)

## Downloads

The PubSub+ Kafka Source Connector is available as a ZIP or TAR package from the [downloads](//solaceproducts.github.io/pubsubplus-connector-kafka-source/downloads/) page.

The package includes jar libraries, documentation with license information and sample property files. Download and extract it into a directory that is on the `plugin.path` of your `connect-standalone` or `connect-distributed` properties file.

## Quick Start

This example demonstrates an end-to-end scenario similar to the [Protocol and API messaging transformations](#protocol-and-api-messaging-transformations) use case, using the WebSocket API to publish a message to the PubSub+ event broker.

It builds on the open source [Apache Kafka Quickstart tutorial](https://kafka.apache.org/quickstart) and walks through getting started in a standalone environment for development purposes. For setting up a distributed environment for production purposes, refer to the User Guide section.

**Note**: The steps are similar if using [Confluent Kafka](//www.confluent.io/download/); there may be difference in the root directory where the Kafka binaries (`bin`) and properties (`etc/kafka`) are located.

**Steps**

1. Install Kafka. Follow the [Apache tutorial](//kafka.apache.org/quickstart#quickstart_download) to download the Kafka release code, start the Zookeeper and Kafka servers in separate command line sessions, then create a topic named `test` and verify it exists.

2. Install PubSub+ Source Connector. Designate and create a directory for the PubSub+ Source Connector - assuming it is named `connectors`. Edit `config/connect-standalone.properties` and ensure the `plugin.path` parameter value includes the absolute path of the `connectors` directory.
[Download]( https://solaceproducts.github.io/pubsubplus-connector-kafka-source/downloads ) and extract the PubSub+ Source Connector into the `connectors` directory.

3. Acquire access to a PubSub+ message broker. If you don't already have one available, the easiest option is to get a free-tier service in a few minutes in [PubSub+ Cloud](//solace.com/try-it-now/) , following the instructions in [Creating Your First Messaging Service](https://docs.solace.com/Solace-Cloud/ggs_signup.htm). 

4. Configure the PubSub+ Source Connector:

   a) Locate the following connection information of your messaging service for the "Solace Java API" (this is what the connector is using inside):
      * Username
      * Password
      * Message VPN
      * one of the Host URIs

   b) edit the PubSub+ Source Connector properties file located at `connectors/pubsubplus-connector-kafka-source-<version>/etc/solace_source.properties`  updating following respective parameters so the connector can access the PubSub+ event broker:
      * `sol.username`
      * `sol.password`
      * `sol.vpn_name`
      * `sol.host`

   **Note**: In the configured source and destination information, the `sol.topics` parameter specifies the ingress topic on PubSub+ (`sourcetest`) and `kafka.topic` is the Kafka destination topic (`test`), created in Step 1.

5. Start the connector in standalone mode. In a command line session run:
   ```sh
   bin/connect-standalone.sh \
   config/connect-standalone.properties \
   connectors/pubsubplus-connector-kafka-source-<version>/etc/solace_source.properties
   ```
   After startup, the logs will eventually contain following line:
   ```
   ================Session is Connected
   ```

6. Start to watch messages arriving to Kafka. See the instructions in the Kafka [tutorial](//kafka.apache.org/quickstart#quickstart_consume) to start a consumer on the `test` topic.

7. Demo time! To generate an event into PubSub+, we use the "Try Me!" test service of the browser-based administration console to publish test messages to the `sourcetest` topic. Behind the scenes, "Try Me!" uses the JavaScript WebSocket API.

   * If you are using PubSub+ Cloud for your messaging service follow the instructions in [Trying Out Your Messaging Service](//docs.solace.com/Solace-Cloud/ggs_tryme.htm).

   * If you are using an existing event broker, log in to its [PubSub+ Manager admin console](//docs.solace.com/Solace-PubSub-Manager/PubSub-Manager-Overview.htm#mc-main-content) and follow the instructions in [How to Send and Receive Test Messages](//docs.solace.com/Solace-PubSub-Manager/PubSub-Manager-Overview.htm#Test-Messages).

   In both cases, ensure to set the topic to `sourcetest`, which the connector is listening to.

   The Kafka consumer from Step 6 should now display the new message arriving to Kafka through the PubSub+ Kafka Source Connector:
   ```
   Hello world!
   ```

## Parameters

The Connector parameters consist of [Kafka-defined parameters](https://kafka.apache.org/documentation/#connect_configuring) and PubSub+ connector-specific parameters.

Refer to the in-line documentation of the [sample PubSub+ Kafka Source Connector properties file](/etc/solace_source.properties) and additional information in the [Configuration](#Configuration) section.

## User Guide

### Deployment

The PubSub+ Source Connector deployment has been tested on Apache Kafka 2.4 and Confluent Kafka 5.4 platforms. The Kafka software is typically placed under the root directory: `/opt/<provider>/<kafka or confluent-version>`.

Kafka distributions may be available as install bundles, Docker images, Kubernetes deployments, etc. They all support Kafka Connect which includes the scripts, tools and sample properties for Kafka connectors.

Kafka provides two options for connector deployment: [standalone mode and distributed mode](//kafka.apache.org/documentation/#connect_running).

* In standalone mode, recommended for development or testing only, configuration is provided together in the Kafka `connect-standalone.properties` and in the PubSub+ Source Connector `solace_source.properties` files and passed to the `connect-standalone` Kafka shell script running on a single worker node (machine), as seen in the [Quick Start](#quick-start).

* In distributed mode, Kafka configuration is provided in `connect-distributed.properties` and passed to the `connect-distributed` Kafka shell script, which is started on each worker node. The `group.id` parameter identifies worker nodes belonging the same group. The script starts a REST server on each worker node and PubSub+ Source Connector configuration is passed to any one of the worker nodes in the group through REST requests in JSON format.

To deploy the Connector, for each target machine, [download]( https://solaceproducts.github.io/pubsubplus-connector-kafka-source/downloads ) and extract the PubSub+ Source Connector into a directory and ensure the `plugin.path` parameter value in the `connect-*.properties` includes the absolute path to that directory. Note that Kafka Connect, i.e., the `connect-standalone` or `connect-distributed` Kafka shell scripts, must be restarted (or equivalent action from a Kafka console is required) if the PubSub+ Source Connector deployment is updated.

Some PubSub+ Source Connector configurations may require the deployment of additional specific files like keystores, truststores, Kerberos config files, etc. It does not matter where these additional files are located, but they must be available on all Kafka Connect Cluster nodes and placed in the same location on all the nodes because they are referenced by absolute location and configured only once through one REST request for all.

#### REST JSON Configuration

First test to confirm the PubSub+ Source Connector is available for use in distributed mode with the command:
```ini
curl http://18.218.82.209:8083/connector-plugins | jq
```

In this case the IP address is one of the nodes running the distributed mode worker process, the port defaults to 8083 or as specified in the `rest.port` property in `connect-distributed.properties`. If the connector is loaded correctly, you should see a response similar to:

```
  {
    "class": "com.solace.connector.kafka.connect.source.SolaceSourceConnector",
    "type": "source",
    "version": "2.1.0"
  },
```

At this point, it is now possible to start the connector in distributed mode with a command similar to:

```ini
curl -X POST -H "Content-Type: application/json" \
             -d @solace_source_properties.json \
             http://18.218.82.209:8083/connectors
``` 

The connector's JSON configuration file, in this case, is called `solace_source_properties.json`. A sample is available [here](/etc/solace_source_properties.json), which can be extended with the same properties as described in the [Parameters section](#parameters).

Determine if the Source Connector is running with the following command:
```ini
curl 18.218.82.209:8083/connectors/solaceSourceConnector/status | jq
```
If there was an error in starting, the details are returned with this command. 

### Troubleshooting

In standalone mode, the connect logs are written to the console. If you do not want to send the output to the console, simply add the "-daemon" option to have all output directed to the logs directory.

In distributed mode, the logs location is determined by the `connect-log4j.properties` located at the `config` directory in the Apache Kafka distribution or under `etc/kafka/` in the Confluent distribution.

If logs are redirected to the standard output, here is a sample log4j.properties snippet to direct them to a file:
```
log4j.rootLogger=INFO, file
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=/var/log/kafka/connect.log
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=[%d] %p %m (%c:%L)%n
log4j.appender.file.MaxFileSize=10MB
log4j.appender.file.MaxBackupIndex=5
log4j.appender.file.append=true
```

To troubleshoot PubSub+ connection issues, increase logging level to DEBUG by adding following line:
```
log4j.logger.com.solacesystems.jcsmp=DEBUG
```
Ensure that you set it back to INFO or WARN for production.

### Event Processing

#### Message Processors

There are many ways to map PubSub+ messages to Kafka topics, partitions, keys, and values, depending on the application behind the events.

The PubSub+ Source Connector comes with two sample message processors that can be used as-is, or as a starting point to develop a customized message processor.

* **SimpleMessageProcessor**: Takes the PubSub+ message as a binary payload and creates a Kafka Source record with a Binary Schema for the value (from the PubSub+ message payload).
* **SampleKeyedMessageProcessor**: A more complex sample that allows the flexibility of changing the source record Key Schema and which value from the PubSub+ message to use as a key. The option of no key in the record is also possible.

The desired message processor is loaded at runtime based on the configuration of the JSON or properties configuration file, for example:
```
sol.message_processor_class=com.solace.connector.kafka.connect.source.msgprocessors.SolSampleSimpleMessageProcessor
```

It is possible to create more custom message processors based on your Kafka record requirements for keying and/or value serialization and the desired format of the PubSub+ event message. Simply add the new message processor classes to the project. The desired message processor is installed at run time based on the configuration file. 

Refer to the [Developers Guide](#developers-guide) for more information about building the Source Connector and extending message processors.

#### Message Replay

By default, the Source Connector will process live events from the PubSub+ event broker. If replay of past recorded events is required for later consumption, it also is possible to use the PubSub+ [Message Replay](//docs.solace.com/Configuring-and-Managing/Message-Replay.htm) feature, initiated through [PubSub+ management](//docs.solace.com/Solace-PubSub-Manager/PubSub-Manager-Overview.htm). 

### Performance and Reliability Considerations

#### Ingesting from PubSub+ Topics

The event broker uses a "best effort" approach to deliver the events from PubSub+ Topics to the Source Connector. If the connector is down, or messages are constantly generated at a rate faster than can be written to Kafka, there is a potential for data loss. When Kafka is configured for its highest throughput, it is susceptible to loss and, obviously, the connector cannot add records if the Kafka broker is unavailable.

When a Kafka Topic is configured for high throughput the use of topics to receive data event messages is acceptable and recommended.

The connector can ingest using a list of topic subscriptions, where each can be a  [wild-card](//docs.solace.com/PubSub-Basics/Wildcard-Charaters-Topic-Subs.htm) subscription.

#### Ingesting from PubSub+ Queues

It is also possible to have the PubSub+ Source Connector attract data events from a  PubSub+ Queue. A queue guarantees order of delivery, provides High Availability and Disaster Recovery (depending on the setup of the PubSub+ brokers) and provides an acknowledgment to the message producer (in this case the PubSub+ event producer application) when the event is stored in all HA and DR members and flushed to disk. This is a higher guarantee than is provided by Kafka even for Kafka idempotent delivery.

When a Kafka Topic is configured with its highest quality-of-service with respect to record loss or duplication, it results in a large reduction in record processing throughput. However, in some application requirements this QoS is required. In this case, the PubSub+ Source Connector should use Queues for the consumption of events from the Event Mesh.

Note that one connector can ingest from only one queue.

##### Recovery from Kafka Connect API or Kafka Broker Failure

When the connector is consuming from a PubSub+ queue, a timed Kafka Connect process commits the source records and offset to disk on the Kafka broker and calls the connector to acknowledge the messages that were processed so far, which removes these event messages from the event broker queue. 

If the Kafka Connect API or the Kafka broker goes down, unacknowledged messages are not lost; they will be retransmitted as soon as the Connect API or Kafka broker is restarted. It is important to note that while the Connect API or the Kafka Broker are off-line, the PubSub+ queue continues to add event messages, so there is no loss of new data from the PubSub+ Event Mesh.

The commit time interval is configurable via the `offset.flush.interval.ms` parameter (default 60,000 ms) in the worker's `connect-distributed.properties` configuration file. If high message rate is expected the parameter shall be tuned, taking into consideration that each task (in case of [Multiple Workers](#multiple-workers)) shall not allow excessively large (for example, 10,000 or more) amount of unacknowledged messages.

##### Queue Handling of Data Bursts

If the throughput through the Connect API is not high enough, and messages are starting to accumulate in the PubSub+ Queue, scaling of the Connector is recommended as discussed [below](#multiple-workers).

If the Source Connector has not been scaled to a sufficient level to deal with bursts, the Queue can act as a "shock absorber" to deal with micro-bursts or sustained periods of heavy event generation in the Event Mesh so that data events are no longer lost due to an unforeseen event.

#### Ingesting From a Queue Configured with Topic-To-Queue Mapping

The Topic-to-Queue Mapping is the simple process of configuring a PubSub+ Queue to attract PubSub+ Topic data event. These data events are immediately available via the queue with a protection against record loss and with the "shock absorber" advantage that use of a Queue provides.

Topic-to-Queue Mapping, just like any PubSub+ topic subscriptions, allows wild-card subscriptions to multiple topics.

#### Multiple Workers

The PubSub+ broker supports far greater throughput than can be afforded through a single instance of the Connect API. The Kafka Broker can also process records at a rate far greater than available through a single instance of the Connector. 
Therefore, multiple instances of the Source Connector can increase throughput from the Kafka broker to the Solace PubSub+ broker.

You can deploy and automatically and spread multiple connector tasks across all available Connect API workers simply by indicating the number of desired tasks in the connector configuration file. 

PubSub+ queue or topic subscriptions must be configured properly to support distributed consumption, so events are automatically load balanced between the multiple workers:

* If the ingestion source is a Queue, it must be configured as [non-exclusive](//docs.solace.com/PubSub-Basics/Endpoints.htm#Queue_Access_Types), which permits multiple consumers to receive messages in a round-robin fashion.

* By the nature of Topics, if there are multiple subscribers to a topic, 
all subscribers receive all of the same topic data event messages. Load balancing can be achieved by applying [Shared Subscriptions](//docs.solace.com/PubSub-Basics/Direct-Messages.htm#Shared), which ensures that messages are delivered to only one active subscriber at a time. 

Note that Shared Subscriptions may need to be [administratively enabled](//docs.solace.com/Configuring-and-Managing/Configuring-Client-Profiles.htm#Allowing-Shared-Subs) in the event broker Client Profile. Also note that Shared Subscriptions are not available on older versions of the event broker. The deprecated [DTO (Deliver-To-One) feature](//docs.solace.com/Configuring-and-Managing/DTO.htm) can be used instead.

### Security Considerations

The security setup and operation between the PubSub+ broker and the Source Connector and Kafka broker and the Source Connector operate completely independently.
 
The Source Connector supports both PKI and Kerberos for more secure authentication beyond the simple user name/password, when connecting to the PubSub+ event broker.

The security setup between the Source Connector and the Kafka brokers is controlled by the Kafka Connect libraries. These are exposed in the configuration file as parameters based on the Kafka-documented parameters and configuration. Please refer to the [Kafka documentation](//docs.confluent.io/current/connect/security.html) for details on securing the Source Connector to the Kafka brokers for both PKI/TLS and Kerberos. 

#### PKI/TLS

The PKI/TLS support is well documented in the [Solace Documentation](//docs.solace.com/Configuring-and-Managing/TLS-SSL-Service-Connections.htm), and will not be repeated here. All the PKI required configuration parameters are part of the configuration variable for the Solace session and transport as referenced above in the [Parameters section](#parameters). Sample parameters are found in the included [properties file](/etc/solace_source.properties). 

#### Kerberos Authentication

Kerberos authentication support requires a bit more configuration than PKI since it is not defined as part of the Solace session or transport. 

Typical Kerberos client applications require details about the Kerberos configuration and details for the authentication. Since the Source Connector is a server application (i.e. no direct user interaction) a Kerberos _keytab_ file is required as part of the authentication, on each Kafka Connect Cluster worker node where the connector is deployed.

The enclosed [krb5.conf](/etc/krb5.conf) and [login.conf](/etc/login.conf) configuration files are samples that allow automatic Kerberos authentication for the Source Connector when it is deployed to the Connect Cluster. Together with the _keytab_ file, they must be also available on all Kafka Connect cluster nodes and placed in the same (any) location on all the nodes. The files are then referenced in the Source Connector properties, for example:
```ini
sol.kerberos.login.conf=/opt/kerberos/login.conf
sol.kerberos.krb5.conf=/opt/kerberos/krb5.conf
```

The following property entry is also required to specify Kerberos Authentication:
```ini
sol.authentication_scheme=AUTHENTICATION_SCHEME_GSS_KRB
```

Kerberos has some very specific requirements to operate correctly. Some additional tips are as follows:
* DNS must be operating correctly both in the Kafka brokers and on the Solace PS+ broker.
* Time services are recommended for use with the Kafka Cluster nodes and the Solace PS+ broker. If there is too much drift in the time between the nodes, Kerberos will fail.
* You must use the DNS name and not the IP address in the Solace PS+ host URI in the Connector configuration file
* You must use the full Kerberos user name (including the Realm) in the configuration property; obviously, no password is required. 

## Developers Guide

### Build the Project

JDK 8 or higher is required for this project.

1. First, clone this GitHub repo:
   ```shell
   git clone https://github.com/SolaceProducts/pubsubplus-connector-kafka-source.git
   cd pubsubplus-connector-kafka-source
   ```
2. Install the test support module:
    ```shell
    git submodule update --init --recursive
    cd solace-integration-test-support
    ./mvnw clean install -DskipTests
    cd ..
    ```
3. Then run the build script:
   ```shell
   gradlew clean build
   ```

This script creates artifacts in the `build` directory, including the deployable packaged PubSub+ Source Connector archives under `build\distributions`.

### Test the Project

An integration test suite is also included, which spins up a Docker-based deployment environment that includes a PubSub+ event broker, Zookeeper, Kafka broker, Kafka Connect. It deploys the connector to Kafka Connect and runs end-to-end tests.

1. Run the tests:
    ```shell
    ./gradlew clean test integrationTest
    ```

### Build a New Message Processor

The processing of the Solace message to create a Kafka source record is handled by [`SolaceMessageProcessorIF`](/src/main/java/com/solace/connector/kafka/connect/source/SolMessageProcessorIF.java). This is a simple interface that creates the Kafka source records from the PubSub+ messages.

To get started, import the following dependency into your project:

**Maven**
```xml
<dependency>
   <groupId>com.solace.connector.kafka.connect</groupId>
   <artifactId>pubsubplus-connector-kafka-source</artifactId>
   <version>2.1.0</version>
</dependency>
```

**Gradle**
```groovy
compile "com.solace.connector.kafka.connect:pubsubplus-connector-kafka-source:2.1.0"
```

Now you can implement your custom `SolMessageProcessorIF`.

For reference, this project includes two examples which you can use as starting points for implementing your own custom message processors:

* [SolSampleSimpleMessageProcessor](/src/main/java/com/solace/connector/kafka/connect/source/msgprocessors/SolSampleSimpleMessageProcessor.java)
* [SolaceSampleKeyedMessageProcessor](/src/main/java/com/solace/connector/kafka/connect/source/msgprocessors/SolaceSampleKeyedMessageProcessor.java)

Once you've built the jar file for your custom message processor project, place it into the same directory as this connector, and update the connector's `sol.message_processor_class` config to point to the class of your new message processor.

More information on Kafka source connector development can be found here:
- [Apache Kafka Connect](https://kafka.apache.org/documentation/)
- [Confluent Kafka Connect](https://docs.confluent.io/current/connect/index.html)

## Additional Information

For additional information, use cases and explanatory videos, please visit the [PubSub+/Kafka Integration Guide](https://docs.solace.com/Developer-Tools/Integration-Guides/Kafka-Connect.htm).

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

See the list of [contributors](../../graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. See the [LICENSE](LICENSE) file for details.

## Resources

For more information about Solace technology in general, please visit these resources:

- How to [Setup and Use](https://solace.com/resources/solace-with-kafka/kafka-source-and-sink-how-to-video-final)
- The [Solace Developers website](https://www.solace.dev/)
- Understanding [Solace technology]( https://solace.com/products/tech/)
- Ask the [Solace Community]( https://solace.community/)
