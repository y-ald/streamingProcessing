package org.yald.application;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobConfig {
    public static StreamExecutionEnvironment setupEnvironment() {
//        Configuration conf = new Configuration();
//        // load from environment or YAML
//        conf.setString("parallelism.default",
//                System.getenv().getOrDefault("PARALLELISM", "1"));
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}