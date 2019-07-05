package com.github.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamFilterTweets {
    private JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        new StreamFilterTweets().run();

    }

    public void run() {
        // Create Properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        // Create a topology

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Input topic

        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter((k, jsonStrig) -> extractUserFollowerInTweet(jsonStrig)>1);

        filteredStream.to("important_tweets");
        // Build a topology

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        // Start a stream application

        kafkaStreams.start();

    }
    private int extractUserFollowerInTweet(String record) {
        try{
        return jsonParser
                .parse(record)
                .getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
