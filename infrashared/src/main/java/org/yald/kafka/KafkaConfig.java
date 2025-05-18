package org.yald.kafka;

import java.util.Properties;
import org.yald.config.KafkaConfigLoader;

public class KafkaConfig {
    private final Properties props;

    public KafkaConfig() {
        this.props = KafkaConfigLoader.load();
        // set defaults if missing
        props.putIfAbsent("bootstrap.servers", "localhost:9092");
        props.putIfAbsent("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    public Properties getProperties() {
        return props;
    }
}
