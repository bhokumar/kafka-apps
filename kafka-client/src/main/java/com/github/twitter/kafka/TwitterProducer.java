package com.github.twitter.kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    String consumerKey = "tfL80tenRFXWseP2XmSbBRqOw";
    String consumerSecret = "CQLE7ehZFXSy6K5VgV5ZAL3GeFRSt5Hig0iA3yKrIQ7FZWhcpd";
    String token = "1100655245791621120-isVlcVqVjvgkmgy7UlxImIPtm5soGO";
    String secret = "0HMgAtLOGmDdb4xEZdSsv9WRxExJkYyGZzLrlhGeN5xBz";

    List<String> terms = Lists.newArrayList("kafka");

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private TwitterProducer() {

    }
    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        LOGGER.info("SETUP");
        BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(100000);
        // Create a twitter client.
        Client hosebirdClient = createTwitterClient(messageQueue);
        hosebirdClient.connect();
        // Create a Kafka producer

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            LOGGER.info("Stopping application.");
            LOGGER.info("Shutting down client from twitter.");
            hosebirdClient.stop();
            LOGGER.info("Closing Producer");
            kafkaProducer.close();
            LOGGER.info("DONE");
        }));
        // loop to send tweets to kafka

        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = messageQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }

            if(msg!=null) {
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                          LOGGER.info("Something bad happened");
                        }
                    }
                });
                LOGGER.info(msg, "has been fetched");
            }
        }

        LOGGER.info("End of Application");

    }

    public Client createTwitterClient(BlockingQueue<String> messageQueue) {

        // Declare the host you want to connect to
        Hosts hoseBirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndPoint = new StatusesFilterEndpoint();

        //List<Long> followings = Lists.newArrayList(1234L, 566788L);

        //hoseBirdEndPoint.followings(followings);
        hoseBirdEndPoint.trackTerms(terms);

        // These secret should be read from a config file
        Authentication hoseBirdAuth = new OAuth1(consumerKey, consumerSecret,token, secret);


        // Create a client

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hoseBirdHosts)
                .authentication(hoseBirdAuth)
                .endpoint(hoseBirdEndPoint)
                .processor(new StringDelimitedProcessor(messageQueue));
        Client hoseBirdClient = builder.build();

        return hoseBirdClient;

    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootStrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

        //properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;

    }
}
