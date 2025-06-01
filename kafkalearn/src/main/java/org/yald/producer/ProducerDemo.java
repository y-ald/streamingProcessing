package org.yald.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.model.Customer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:49092,localhost:29092,localhost:39092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.yald.deser.CustomerSerializer");
        props.put("schema.registry.url", "http://localhost:8082");

        Producer<String, Customer> producer = new KafkaProducer<>(props);
        for (int i = 20; i < 30; i++) {
            try {
                Customer customer = new Customer(i, Integer.toString(i));
                RecordMetadata response = producer.send(new ProducerRecord<String, Customer>("my-topic", Integer.toString(i), customer)).get();
                log.info("Message sent successfully with key: {}, value: {}, partition: {}, offset: {}",
                        Integer.toString(i), customer, response.partition(), response.offset());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (RetriableException e) {
                log.warn("Retriable exception occurred, retrying...");
                // In a real application, you might want to implement a retry mechanism here
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        producer.close();
    }
}