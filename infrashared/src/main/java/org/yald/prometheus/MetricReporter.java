package org.yald.prometheus;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MetricReporter {
    private final PrometheusReporter reporter = new PrometheusReporter();

    public void start(StreamExecutionEnvironment env) {
        // assume JMX beans already exposed by Flink
        reporter.start(9249);
    }
}
