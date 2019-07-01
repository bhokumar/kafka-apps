package com.github.kafka.version1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {
        // Create Producer properties
        String bootStrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

        //properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        // Send the data
        for(int i =0;i<10;i++) {
            // Create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello world"+Integer.valueOf(i));
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // Executes every time a record is successfully sent or exception occurred.
                    if(exception ==null) {
                        // the record was successfully sent.
                        logger.info("Received metadata. \n"+
                                "Topic : " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "TimeStamp: " + metadata.timestamp());
                    } else {
                        logger.error("Error while producing record");
                    }
                }
            });

            //producer.flush();
        }

        producer.flush();
        producer.close();
    }
}
