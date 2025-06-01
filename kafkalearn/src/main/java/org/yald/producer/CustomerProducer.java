package org.yald.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.model.Customer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CustomerProducer {
    private static final Logger log = LoggerFactory.getLogger(CustomerProducer.class);

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

        String topic = "customerContacts";

        InputStream schemaStream = CustomerProducer.class.getClassLoader().getResourceAsStream("avroschema/customer.avsc");

        try (Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props)) {

            Schema schema = null;
            try {
                schema = new Schema.Parser().parse(schemaStream);
            } catch (IOException e) {
                log.error("Failed to parse Avro schema", e);
                throw new RuntimeException(e);
            }

            for (int nCustomers = 0; nCustomers < 20; nCustomers++) {
                String name = "exampleCustomer" + nCustomers;
                String email = "example " + nCustomers + "@example.com";
                GenericRecord customer = new GenericData.Record(schema);
                customer.put("id", nCustomers);
                customer.put("name", name);
                customer.put("email", email);

                ProducerRecord<String, GenericRecord> data =
                        new ProducerRecord<>("customerContacts", name, customer);
                producer.send(data);
                log.info("Sent customer: id={}, name={}, email={}", nCustomers, name, email);
            }
        }

    }
}
