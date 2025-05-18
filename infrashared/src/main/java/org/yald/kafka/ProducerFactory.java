package org.yald.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class ProducerFactory<T> {

    private final KafkaConfig kafkaConfig;
    private final String topic;
    private final DeliveryGuarantee deliveryGuarantee;

    public ProducerFactory(String topic, DeliveryGuarantee deliveryGuarantee) {
        this.topic = topic;
        this.kafkaConfig = new KafkaConfig();
        this.deliveryGuarantee = deliveryGuarantee;

    }

    public KafkaSink<T> getKafkaSink(SerializationSchema<T> valueSerializationSchema) {
        return KafkaSink.<T>builder()
                .setKafkaProducerConfig(kafkaConfig.getProperties())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(valueSerializationSchema)
                        .build()
                )
                .setDeliveryGuarantee(deliveryGuarantee)
                .build();
    }
}
