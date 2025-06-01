package org.yald.deser;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.yald.model.Customer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public Customer deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    @Override
    public Customer deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) return null;
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int customerID = buffer.getInt();
            int nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String customerName = new String(nameBytes, "UTF-8");
            return new Customer(customerID, customerName);
        } catch (Exception e) {
            throw new SerializationException("Erreur lors de la désérialisation de Customer", e);
        }
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}