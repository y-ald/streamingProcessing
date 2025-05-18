package org.yald.prometheus;


public final class MetricDefinitions {
    private MetricDefinitions() {}
    public static final String JOB_LATENCY = "flink_job_latency_ms";
    public static final String RECORDS_PROCESSED = "records_processed_total";
    // add domain-specific metrics...
}
