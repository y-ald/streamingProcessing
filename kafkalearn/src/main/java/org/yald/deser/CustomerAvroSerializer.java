package org.yald.deser;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.yald.avro.Customer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class CustomerAvroSerializer implements Serializer<Customer> {
    @Override
    public byte[] serialize(String topic, Customer customer) {
        byte[] arr = new byte[100000];
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
                GenericDatumWriter<Customer> writer = new GenericDatumWriter<>(customer.getSchema());
                writer.write(customer, binaryEncoder);
                binaryEncoder.flush();
                arr = outputStream.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }

        return arr;
    }
}
