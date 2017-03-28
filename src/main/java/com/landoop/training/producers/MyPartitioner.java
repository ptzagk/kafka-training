package com.landoop.training.producers;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

public class MyPartitioner implements Partitioner {

    private CustomerService customerService;

    public MyPartitioner() {
        customerService = new CustomerService();
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        int partition = 0;
        String userName = (String) key;

        // Find the id of current user based on the username
        Integer userId = customerService.findUserId(userName);

        // If the userId not found, default partition is 0
        if (userId != null) {
            partition = userId;
        }
        return partition;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
