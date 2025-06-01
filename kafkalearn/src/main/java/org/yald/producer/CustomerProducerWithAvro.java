package org.yald.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.avro.Customer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CustomerProducerWithAvro {
    private static final Logger log = LoggerFactory.getLogger(CustomerProducerWithAvro.class);

    public static void main(String[] args) {
        log.info("I am a Kafka CustomerProducer");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:49092,localhost:29092,localhost:39092");
        props.put("acks", "all");
        props.put("key.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer",
                "org.yald.deser.CustomerAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8082");

        String topic = "customerContacts";


        try (Producer<String, Customer> producer = new KafkaProducer<String, Customer>(props)) {


            for (int nCustomers = 0; nCustomers < 20; nCustomers++) {
                String name = "exampleCustomer" + nCustomers;
                String email = "example " + nCustomers + "@example.com";
                Customer customer = Customer.newBuilder().setId(nCustomers).setName(name).setEmail(email).build();

                ProducerRecord<String, Customer> data =
                        new ProducerRecord<>("customerContacts", name, customer);
                producer.send(data);
                log.info("Sent customer: id={}, name={}, email={}", nCustomers, name, email);
            }
        }

    }
}
