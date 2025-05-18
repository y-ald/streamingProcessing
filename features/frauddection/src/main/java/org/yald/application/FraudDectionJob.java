package org.yald.application;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.deser.TransactionDeserializationSchema;
import org.yald.domain.Alert;
import org.yald.deser.AlertSerializationSchema;
import org.yald.domain.Transaction;
import org.yald.kafka.ConsumerFactory;
import org.yald.kafka.ProducerFactory;

public class FraudDectionJob {
    private final StreamExecutionEnvironment streamExecutionEnvironment;
    private static final Logger logger
            = LoggerFactory.getLogger(TransactionGenerationJob.class);

    public FraudDectionJob(StreamExecutionEnvironment streamExecutionEnvironment) {
        this.streamExecutionEnvironment = streamExecutionEnvironment;
    }

    public void execute() {
        KafkaRecordDeserializationSchema<Transaction> kafkaRecordDeserializationSchema = KafkaRecordDeserializationSchema.valueOnly(new TransactionDeserializationSchema());
        KafkaSource<Transaction> kafkaSource = new ConsumerFactory<>("transaction", kafkaRecordDeserializationSchema, OffsetsInitializer.earliest()).getKafkaSource();
        DataStreamSource<Transaction> transactionDataStreamSource =
                streamExecutionEnvironment.fromSource(kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Transaction Source");

        logger.info("************************ Fraud Data Stream Created ************************");

        DataStream<Alert> alerts = transactionDataStreamSource
                .keyBy(Transaction::accountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        logger.info("************************ Fraud Data Stream Processed ************************");
        ProducerFactory<Alert> producerFactor = new ProducerFactory<>("alerts", DeliveryGuarantee.AT_LEAST_ONCE);
        alerts.sinkTo(producerFactor.getKafkaSink(new AlertSerializationSchema())).name("Kafka Sink");
        logger.info("************************ Fraud Data Stream Sink Created ************************");
        try {
            streamExecutionEnvironment.execute("Fraud Detection Job");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
