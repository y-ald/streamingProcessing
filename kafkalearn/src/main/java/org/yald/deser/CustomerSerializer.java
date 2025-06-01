package org.yald.deser;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.yald.model.Customer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Customer customer) {
        return serialize(topic, null, customer);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Customer data) {
        try {
            byte[] serializedName;
            int stringSize;
            if (data == null)
                return null;
            else {
                if (data.customerName() != null) {
                    serializedName = data.customerName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(data.customerID());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException(
            "Error when serializing Customer to byte[] " + e);
        }
    }

    @Override
    public void close() {
        Serializer.super.close();
    }
}
