package org.yald.kafka;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;

public class ConsumerFactory<T> {
    private final KafkaConfig kafkaConfig;
    private final String topic;
    private final KafkaRecordDeserializationSchema<T> deserializationSchema;
    private final OffsetsInitializer offsetsInitializer;

    public ConsumerFactory(String topic, KafkaRecordDeserializationSchema<T> deserializationSchema, OffsetsInitializer offsetsInitializer) {
        this.topic = topic;
        this.deserializationSchema = deserializationSchema;
        this.kafkaConfig = new KafkaConfig();
        this.offsetsInitializer = offsetsInitializer;
    }

    public KafkaSource<T> getKafkaSource() {

        return KafkaSource.<T>builder()
                .setBootstrapServers(kafkaConfig.getProperties().getProperty("bootstrap.servers"))
                .setTopics(topic)
                .setProperties(this.kafkaConfig.getProperties())
                .setDeserializer(deserializationSchema)
                .setStartingOffsets(offsetsInitializer).build();
    }

    public KafkaSource<T> getKafkaSource(String groupId) {

        return KafkaSource.<T>builder()
                .setTopics(topic)
                .setProperties(this.kafkaConfig.getProperties())
                .setDeserializer(deserializationSchema)
                .setStartingOffsets(offsetsInitializer)
                .setGroupId(groupId).build();
    }
}
