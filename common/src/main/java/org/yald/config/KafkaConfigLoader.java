package org.yald.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class KafkaConfigLoader {
    private static final String CONFIG_FILE = "application.yaml";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Properties load() {
        try (InputStream in = KafkaConfigLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            Map<String,Object> yaml = new Yaml().load(in);
            Map<String,String> kafka = (Map<String,String>) yaml.get("kafka");
            Properties props = new Properties();
            kafka.forEach(props::setProperty);
            // override with env vars if present
            System.getenv().forEach((k,v) -> {
                if (k.startsWith("KAFKA_")) {
                    props.setProperty(k.substring(6).toLowerCase(), v);
                }
            });
            return props;
        } catch (Exception e) {
            throw new RuntimeException("Failed to load Kafka config", e);
        }
    }
}