package com.landoop.training.producers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer>{

    public void configure(Map<String, ?> map, boolean b) {

    }

    /**
     *

     4 byte int representing customerId
     4 byte int representing length of customerName in UTF-8 bytes (0 if name is
     Null)
     N bytes representing customerName in UTF-8

     * @param topic
     * @param customer
     * @return
     */
    public byte[] serialize(String topic, Customer customer) {

        try {

            byte[] serializedName = new byte[0];
            int stringSize = 0;

            if (customer == null)
                return null;
            else {
                if (customer.getCustomerName() != null) {
                    serializedName = customer.getCustomerName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                } else {

                }
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
            buffer.putInt(customer.getCustomerId());
            buffer.putInt(stringSize);
            buffer.put(serializedName);
            return buffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[] " + e);
        }
    }

    public void close() {

    }
}
