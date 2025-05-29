package org.yald.application;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class JobConfig {
    public static StreamExecutionEnvironment setupEnvironment() {
        Configuration conf = GlobalConfiguration.loadConfiguration("orchestration/src/main/resources");
        // load from environment or YAML
        conf.setString("parallelism.default",
                System.getenv().getOrDefault("PARALLELISM", "1"));
        return StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }
}