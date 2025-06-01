package org.yald.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.yald.model.Customer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static final Logger log = org.slf4j.LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:49092,localhost:29092,localhost:39092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.yald.deser.CustomerDeserializer");
        props.put("schema.registry.url", "http://localhost:8082");
        props.put("auto.offset.reset", "latest");
        // Create a consumer instance
        KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList("my-topic"));

        // Poll for new messages
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
    }
}
