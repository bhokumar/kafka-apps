package com.github.kafka.version1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {
    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){

    }

    private void run() {
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        CountDownLatch countDownLatch = new CountDownLatch(1);

        // Creating the consumer thread
        Runnable consumerTask = new ConsumerTask(
                bootStrapServers,
                groupId,
                topic,
                countDownLatch
        );

        Thread consumerThread = new Thread(consumerTask);
        consumerThread.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () -> {
                    logger.info("Caught shutdown hook");
                    ((ConsumerTask) consumerTask).shutdown();
                    try{
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    logger.info("Application has exited!");
                }
        ));
        try{
        countDownLatch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerTask implements Runnable {

        private CountDownLatch countDownLatch;
        private KafkaConsumer<String, String> kafkaConsumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerTask.class);

        public ConsumerTask(String bootStrapServers,
                              String groupId,
                              String topic,
                              CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            this.kafkaConsumer = new KafkaConsumer<String, String>(properties);

            kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : consumerRecords) {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition(), "OffSet: " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");
            } finally {
                kafkaConsumer.close();

                // Tell our main code that we are done with consumer
                countDownLatch.countDown();
            }
        }

        public void shutdown() {
            // the wakeup method is a special method to interrupt consumer.poll().
            // It will throw Exception WakeUpException.
            kafkaConsumer.wakeup();
        }
    }
}

