package org.yald.deser;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;
import org.yald.avro.Customer;

public class CustomerAvroDeserializer implements Deserializer<Customer> {
    @Override
    public Customer deserialize(String topic, byte[] data) {

        try {
            if (data != null) {
                DatumReader<Customer> reader = new SpecificDatumReader<>(Customer.getClassSchema());
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                return reader.read(null, decoder);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

