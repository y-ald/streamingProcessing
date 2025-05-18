package org.yald.deser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.yald.domain.Alert;

public class AlertSerializationSchema implements SerializationSchema<Alert> {
    private static final ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

    @Override
    public byte[] serialize(Alert alert) {
        try {
            return objectMapper.writeValueAsBytes(alert);
        } catch (Exception e) {
            throw new RuntimeException("Erreur de sérialisation d'Alert", e);
        }
    }
}
