package org.yald.application;


import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yald.domain.Alert;
import org.yald.domain.Transaction;
import org.yald.kafka.ConsumerFactory;
import org.yald.prometheus.MetricReporter;

public class FlinkJobLauncher {
    private static final Logger logger
            = LoggerFactory.getLogger(FlinkJobLauncher.class);

    public static void main(String[] args) {
        var env = JobConfig.setupEnvironment();
        // start metrics
        new MetricReporter().start(env);

        new TransactionGenerationJob(env).execute();
        new FraudDectionJob(env).execute();
    }
}
