package org.yald.deser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.yald.domain.Transaction;

public class TransactionDeserializationSchema implements DeserializationSchema<Transaction> {

    @Override
    public Transaction deserialize(byte[] message) {
        try {
            return new ObjectMapper().readValue(message, Transaction.class);
        } catch (Exception e) {
            throw new RuntimeException("Erreur de désérialisation de Transaction", e);
        }
    }

    @Override
    public boolean isEndOfStream(Transaction transaction) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
