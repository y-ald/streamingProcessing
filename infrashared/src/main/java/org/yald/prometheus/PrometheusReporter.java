package org.yald.prometheus;

import io.prometheus.client.exporter.HTTPServer;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;

public class PrometheusReporter {
    private HTTPServer server;

    public void start(int port) {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            // assume JMX exporter jar configured as javaagent
            server = new HTTPServer(port);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start Prometheus reporter", e);
        }
    }

    public void stop() {
        if (server != null) server.stop();
    }
}