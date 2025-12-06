package edu.supmti.kafka;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WordProducer {
    public static void main(String[] args) {
        String topicName = "WordCount-Topic";
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        System.out.println("Enter text (type 'exit' to quit):");
        while (true) {
            String input = scanner.nextLine();
            if ("exit".equalsIgnoreCase(input)) break;

            String[] words = input.split("\\W+");
            for (String word : words) {
                if (!word.isEmpty()) {
                    producer.send(new ProducerRecord<>(topicName, word, word));
                }
            }
            System.out.println("Words sent to Kafka.");
        }
        
        producer.close();
        scanner.close();
    }
}