package com.github.elasticsearch.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerWithAutoCommitFalse {
    private final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerWithAutoCommitFalse.class.getName());

    private JsonParser jsonParser = new JsonParser();

    private RestHighLevelClient createClient() {
        String hostName="elastic-cluster-8771589336.ap-southeast-2.bonsaisearch.net";
        String userName="pqkodmr8tj";
        String password="fgiuvsq1h4";

        final CredentialsProvider  credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostName, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback(){
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);
        return restHighLevelClient;
    }

    public KafkaConsumer<String, String> createConsumer(String topic) {
        // Create Consumer properties
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";
        //String topic = "twitter_tweets";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");

        //properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Consumer

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    private void run() throws IOException {
        RestHighLevelClient highLevelClient = createClient();

        //String jsonString = "{\"foo\": \"bar\"}";


        KafkaConsumer<String, String> kafkaConsumer = createConsumer("twitter_tweets");

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            int recordCount = records.count();

            logger.info("Received " + recordCount+ " records.");
            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String, String> record: records) {
                // Kafka generic id
                //String id= record.topic() +"_"+ record.partition()+"_" + record.offset();
                try {
                    String id = extractIdFromTweet(record.value());
                    IndexRequest indexRequest = new IndexRequest("twitter2", "tweets", id).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); // We add to our bulk request(It takes no time.)

                    IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

                    String generatedId = indexResponse.getId();

                    logger.info(id);
                }catch (Exception e) {
                    logger.warn("Skipping bad data " + record.value());
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (recordCount > 0) {
                BulkResponse bulkItemResponses = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed.");

                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }

        //highLevelClient.close();
    }

    private String extractIdFromTweet(String record) {
        return jsonParser.parse(record).getAsJsonObject().get("id_str").getAsString();
    }
    public static void main(String[] args) throws IOException {
        new ElasticSearchConsumerWithAutoCommitFalse().run();
    }
}
