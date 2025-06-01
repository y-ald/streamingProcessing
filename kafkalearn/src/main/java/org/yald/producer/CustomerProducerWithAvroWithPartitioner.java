package org.yald.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.avro.Customer;

import java.util.Properties;

public class CustomerProducerWithAvroWithPartitioner {
    private static final Logger log = LoggerFactory.getLogger(CustomerProducerWithAvroWithPartitioner.class);

    public static void main(String[] args) {
        log.info("I am a Kafka CustomerProducer");

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:49092,localhost:29092,localhost:39092");
        props.put("acks", "all");
        props.put("key.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer",
                "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8082");
        props.put("partitioner.class", "org.yald.partitioner.CustomerPartitioner");
        props.put("interceptor.classes", "org.yald.interceptor.CustomerInterceptor");

        String topic = "customerContactsWithPartitioner";


        try (Producer<Integer, Customer> producer = new KafkaProducer<Integer, Customer>(props)) {


            for (int nCustomers = 0; nCustomers < 20; nCustomers++) {
                String name = "exampleCustomer" + nCustomers;
                String email = "example " + nCustomers + "@example.com";
                Customer customer = Customer.newBuilder().setId(nCustomers).setName(name).setEmail(email).build();

                ProducerRecord<Integer, Customer> data =
                        new ProducerRecord<>(topic, nCustomers, customer);
                producer.send(data);
                log.info("Sent customer: id={}, name={}, email={}", nCustomers, name, email);
            }
        }

    }
}
