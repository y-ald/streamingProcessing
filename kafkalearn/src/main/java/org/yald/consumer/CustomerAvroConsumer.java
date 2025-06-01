package org.yald.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.avro.Customer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CustomerAvroConsumer {
    public static final Logger log = LoggerFactory.getLogger(CustomerAvroConsumer.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:49092,localhost:29092,localhost:39092");
        props.put("group.id", "test-group2");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8082");
        props.put("auto.offset.reset", "earliest");



        // This is where you would implement the consumer logic
        // For example, setting up Kafka consumer properties, subscribing to topics, etc.
        System.out.println("Customer Avro Consumer is running...");
        String topic = "customerContacts";

        // Poll for new messages
        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));
        try {
            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, Customer> record : records) {
                    log.info("Consumed message: key={}, value={}, offset={}",
                            record.key(), record.value(), record.offset());
                    System.out.printf("Consumed message: key=%s, value=%s, offset=%d%n",
                            record.key(), record.value(), record.offset());
                }
            }
        } finally {
            consumer.close();
        }
        // Note: Actual implementation would require Kafka dependencies and setup
    }
}
