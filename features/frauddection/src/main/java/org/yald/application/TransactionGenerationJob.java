package org.yald.application;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.deser.TransactionSerializationSchema;
import org.yald.domain.Transaction;
import org.yald.infrastructure.TransactionDataSource;
import org.yald.kafka.ProducerFactory;

public class TransactionGenerationJob {
    private final StreamExecutionEnvironment streamExecutionEnvironment;
    private final TransactionDataSource  transactionDataSource;
    private static final Logger logger
            = LoggerFactory.getLogger(TransactionGenerationJob.class);

    public TransactionGenerationJob(StreamExecutionEnvironment streamExecutionEnvironment) {
        this.streamExecutionEnvironment = streamExecutionEnvironment;
        this.transactionDataSource = new TransactionDataSource();
    }

    public void execute() {

        DataStream<Transaction> transactionDataStream =  streamExecutionEnvironment.fromSource(transactionDataSource.getTransactionDataGeneratorSource(), WatermarkStrategy.noWatermarks(), "Transaction Data Source")
                .name("Transaction Data Source")
                .uid("Transaction Data Source")
                .setParallelism(1);

        logger.info("************************ Transaction Data Stream Created ************************");

        ProducerFactory<Transaction> transactionProducerFactory = new ProducerFactory<>("transaction", DeliveryGuarantee.AT_LEAST_ONCE);
        transactionDataStream.print().name("Transaction Data Stream Print").uid("Transaction Data Stream Print").setParallelism(1);
        transactionDataStream.sinkTo(transactionProducerFactory.getKafkaSink(new TransactionSerializationSchema()))
                .name("Kafka Sink")
                .uid("Kafka Sink")
                .setParallelism(1);

        logger.info("************************ Transaction Data Stream Sink Created ************************");
        executeJob();
    }

    private void executeJob() {
        try {
            streamExecutionEnvironment.execute("Transaction Generation Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
