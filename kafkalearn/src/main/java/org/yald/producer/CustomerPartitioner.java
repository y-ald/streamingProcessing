package org.yald.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomerPartitioner implements Partitioner {
    public static Logger log = LoggerFactory.getLogger(CustomerPartitioner.class);

    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        int partionNumber = 0;

        List<PartitionInfo> partitions = cluster.partitionsForTopic(s);

        int numPartitions = partitions.size();
        if (o instanceof Integer) {
            // Use the customer ID as the partition key
            int customerId = (Integer) o;
            partionNumber = Math.abs(customerId) % numPartitions;
            log.info("Partitioning by customer ID: {} -> Partition: {}", customerId, partionNumber);
            return partionNumber; // Ensure non-negative partition index
        } else if (o instanceof String) {
            // Use the customer name as the partition key
            String customerName = (String) o;
            partionNumber = Math.abs(customerName.hashCode()) % numPartitions;
            log.info("Partitioning by customer name: {} -> Partition: {}", customerName, partionNumber);
            return partionNumber; // Ensure non-negative partition index
        }

        return partionNumber;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
