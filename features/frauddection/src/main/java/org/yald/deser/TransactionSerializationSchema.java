package org.yald.deser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.yald.domain.Transaction;

public class TransactionSerializationSchema implements SerializationSchema<Transaction> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Transaction transaction) {
        try {
            return objectMapper.writeValueAsBytes(transaction);
        } catch (Exception e) {
            throw new RuntimeException("Erreur de sérialisation d'Alert", e);
        }
    }
}
