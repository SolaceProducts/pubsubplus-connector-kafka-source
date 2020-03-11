package com.solace.messaging.kafka.it;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

public class TestKafkaConsumer {

    // Queue to communicate received messages
    public static BlockingQueue<ConsumerRecord<Object, Object>> kafkaReceivedMessages  = new ArrayBlockingQueue<>(10);
    
    private Runnable myConsumerRunnable;
    private String kafkaTopic;
    Logger logger = LoggerFactory.getLogger(TestKafkaConsumer.class.getName());
    CountDownLatch latch = new CountDownLatch(1);
    
    public TestKafkaConsumer(String kafkaTestTopic) {
        kafkaTopic = kafkaTestTopic;
    }

    public void run() {
        String bootstrapServers = MessagingServiceFullLocalSetupConfluent.COMPOSE_CONTAINER_KAFKA.getServiceHost("kafka_1", 39092)
                        + ":39092";
        String groupId = "test";

        // latch for dealing with multiple threads

        // create the consumer runnable
        logger.info("Creating the consumer thread");
        myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, kafkaTopic, latch);

        // start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        logger.info("Stopping consumer");
        ((ConsumerRunnable) myConsumerRunnable).shutdown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Consumer has been stoppped");
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<Object, Object> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

        public ConsumerRunnable(String bootstrapServers, String groupId, String topic, CountDownLatch latch) {
            this.latch = latch;

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteBufferDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            // create consumer
            consumer = new KafkaConsumer<Object, Object>(properties);
            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            // poll for new data
            try {
                while (true) {
                    ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofMillis(100));
                    latch.countDown();
                    for (ConsumerRecord<Object, Object> record : records) {
                        kafkaReceivedMessages.put(record);
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } catch (InterruptedException e) {
                 e.printStackTrace();
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
