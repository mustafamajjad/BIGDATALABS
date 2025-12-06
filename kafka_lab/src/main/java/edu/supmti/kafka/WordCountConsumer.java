package edu.supmti.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WordCountConsumer {
    public static void main(String[] args) {
        String topicName = "WordCount-Topic";
        Map<String, Integer> wordCounts = new HashMap<>();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "wordcount-group");
        props.put("enable.auto.commit", "true");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Waiting for words...");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String word = record.value().toLowerCase();
                    wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
                    
                    // Display frequency in real-time 
                    System.out.println("Word Frequencies: " + wordCounts);
                }
            }
        } finally {
            consumer.close();
        }
    }
}